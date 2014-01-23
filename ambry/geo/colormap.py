'''
Created on Mar 17, 2013

@author: eric
'''

def _load_maps():
    import os.path
    import ambry.support
    import csv
    
    def cmap_line(row):
        return {
                'num': int(row['ColorNum'].strip()),
                'letter': row['ColorLetter'].strip(),
                'R': int(row['R'].strip()),
                'G': int(row['G'].strip()),
                'B': int(row['B'].strip()),                
                
                }
    
    sets = {}
    cset = None
    cset_key = None
    scheme = None
    with open(os.path.join(os.path.dirname(ambry.support.__file__),'colorbrewerschemes.csv')) as f:
        for row in csv.DictReader(f):
            try:
                if row['SchemeType'].strip():
                    scheme_type = row['SchemeType'].strip()
                
                if row['ColorName'].strip():
                    if cset_key:
                        sets[cset_key] = cset 
                    cset_key = (row['ColorName'].strip(), int(row['NumOfColors'].strip()))
                    cset = {
                            'crit_val':row['CritVal'].strip(),
                            'scheme_type':scheme_type.lower(),
                            'n_colors':int(row['NumOfColors'].strip()),
                            'map' : []
                            }
                    cset['map'].append(cmap_line(row))
                else:
                    cset['map'].append(cmap_line(row))
            except Exception as e:
                print "Error in row: ", row, e.message  
                
    return sets  
def test_colormaps():

    sets = _load_maps()   
    nums = set()
    for key, s in sets.items():
        nums.add(s['n_colors'])
        
    print nums
    
def get_colormaps():
    """Return a dictionary of all colormaps"""    
    return _load_maps()

def get_colormap(name=None, n_colors=None, expand = None,reverse=False):
    """Get a colormap by name and number of colors, 
    n_colors must be in the range of 3 to 12, inclusive
    
    expand multiplies the size of the colormap by interpolating that number of additional
    colors. n=1 will double the size of the map, n=2 will tripple it, etc. 
    
    See http://colorbrewer2.org/ for the color browser
    """
        
    cmap =  get_colormaps()[(name,int(n_colors))]
    
    if expand:
        expand_map(cmap, expand)
    
    if reverse:
        cmap['map'].reverse()
        
    return cmap

def interp1(value, maximum, rng):
    
    ''' Return the v/m point in the range rng'''
    return rng[0] + (rng[1] - rng[0])*float(value)/float(maximum)

def interp3(value, maximum, s, e):
    from functools import partial
    return map(partial(interp1, value, maximum), zip(s,e))

def expand_map(cmap, n):
    import colorsys
    
    o = dict(cmap)
    o['map'] = {}
    omap = []
    imap = cmap['map']
    for j in range(len(imap)-1):
        omap.append(imap[j])
        s = imap[j]
        e = imap[j+1]
        st = (s['R'],s['G'],s['B'])
        et = (e['R'],e['G'],e['B'])   
        for i in range(1,n+1):
            r = interp3(i,n+1, st,et)
            omap.append({'R':int(r[0]),'G':int(r[1]),'B':int(r[2])})
     
    omap.append(imap[j+1])
   
        
    for i in range(len(omap)):
        
        if omap[i].get('letter'):
            letter = omap[i]['letter']
            
        omap[i]['letter'] = letter+str(i+1)
        omap[i]['num'] = i+1

    o['n_colors'] = len(omap)
    o['map'] = omap

    return o
        

def geometric_breaks(n, min, max):
    """Produce breaks where each is two times larger than the previous"""

    n -= 1

    parts = 2**n-1
    step = (max - min) / parts
    
    breaks = []
    x = min
    for i in range(n):
        breaks.append(x)
        x += step*2**i
        
    breaks.append(max)
    return breaks

def logistic_breaks(n, min, max, reversed = False):
    import numpy as np
    ex = np.exp(-(1.0/3000.0)*np.square(np.arange(101)))[::-1]
    ex = ex - np.min(ex)
    ex = ex / np.max(ex)

    o = []
    for v in range(n):
        
        idx = int(float(v)/float(n-1) * 100)
        
        if reversed:
            v = ex[100-idx]
        else:
            v = ex[idx]
            
        o.append(v*(float(max)-float(min)) + min)
            
    return o
    
def exponential_breaks(n, avg):
    o = []
    for i in range(-n/2,n/2):
        o.append(avg*(2**i+2**(i+1))/2.0)

    return o

def write_colormap(file_name, a, map, break_scheme='even', min_val=None, max_val =None, ave_val=None):
    """Write a QGIS colormap file"""
    import numpy as np
    import numpy.ma as ma
    import math

    header ="# QGIS Generated Color Map Export File\nINTERPOLATION:DISCRETE\n"
    
    masked = ma.masked_equal(a, 0)
    
    min_ = np.min(masked) if not min_val else min_val
    max_ = np.max(a) if not max_val else max_val
    ave_ = masked.mean() if not ave_val else ave_val

    if break_scheme == 'even':
        max_ = max_ * 1.001 # Be sure to get all values
        range = min_-max_
        delta = range*.001
        r = np.linspace(min_-delta, max_+delta, num=map['n_colors']+1)
    elif break_scheme == 'jenks':
        from ambry.geo import jenks_breaks
        r = jenks_breaks(a, map['n_colors'])
    elif break_scheme == 'geometric':
        r = geometric_breaks(map['n_colors'], min_, max_)
    elif break_scheme == 'logistic':
        r = logistic_breaks(map['n_colors'], min_, max_)
    elif break_scheme == 'exponential':
        r = exponential_breaks(map['n_colors'], ave_)
    elif break_scheme == 'stddev':
        sd = np.std(a)
    else:
        raise Exception("Unknown break scheme: {}".format(break_scheme))
    
    colors = map['map']
    
    colors.append(None) # Causes the last item to be skipped

    alphas, alpha_step = np.linspace(64,255,len(colors),retstep=True)
    alpha = alpha_step+64

    with open(file_name, 'w') as f:
        f.write(header)
        last_me = None
        for v,me in zip(r,colors):
            if me:
              
                f.write(','.join([str(v),str(me['R']), str(me['G']), str(me['B']), str(int(alpha)), me['letter'] ]))
                alpha += alpha_step
                alpha = min(alpha, 255)
                f.write('\n')
                last_me = me
    
        # Prevents 'holes' where the value is higher than the max_val
        if max_val:
            v = np.max(a)
            f.write(','.join([str(v),str(last_me['R']), str(last_me['G']), str(last_me['B']), str(int(alpha)), last_me['letter'] ]))
            f.write('\n')
    
    
    