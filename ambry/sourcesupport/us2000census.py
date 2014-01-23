'''
Created on Aug 19, 2012

@author: eric
'''
from  ambry.sourcesupport.uscensus import UsCensusDimBundle
from  ambry.sourcesupport.uscensus import UsCensusFactBundle

class Us2000CensusDimBundle(UsCensusDimBundle):
    '''
    Bundle code for US 2000 Census, Summary File 1
    '''

    def __init__(self,directory=None):
        
        
        self.super_ = super(Us2000CensusDimBundle, self)
        self.super_.__init__(directory)
        
        
    def _scrape_urls(self, rootUrl, states_file, suffix='_uf1'):
        '''Extract all of the URLS from the Census website and store them'''
        import urllib
        import urlparse
        import re
        from bs4 import BeautifulSoup
    
        log = self.log
        tick = self.ptick
    
        # Load in a list of states, so we know which links to follow
        with open(states_file) as f:
            states = map(lambda s: s.strip(),f.readlines())
        
        # Root URL for downloading files. 
       
        self.log('Getting URLS from '+rootUrl)
       
        doc = urllib.urlretrieve(rootUrl)
  
        # Get all of the links
        log('S = state, T = segment table, g = geo')
        tables = {}
        geos = {}
       
        with open(doc[0]) as df:
            for link in BeautifulSoup(df).find_all('a'):
                tick('S')
                if not link.get('href') or not link.string or not link.contents:
                    continue# Didn't get a sensible link
                # Only descend into links that name a state
                state = link.get('href').strip('/')
              
                if link.string and link.contents[0] and state in states :
                    stateUrl = urlparse.urljoin(rootUrl, link.get('href'))
                    stateIndex = urllib.urlretrieve(stateUrl)
                    # Get all of the zip files in the directory
                    
                    with open(stateIndex[0]) as f:          
                        for link in  BeautifulSoup(f).find_all('a'):

                            if link.get('href') and  '.zip' in link.get('href'):
                                final_url = urlparse.urljoin(stateUrl, link.get('href')).encode('ascii', 'ignore')
                                if 'geo'+suffix in final_url:
                                    tick('g')
                                    state = re.match('.*/(\w{2})geo'+suffix, final_url).group(1)
                                    geos[state] = final_url

            
        return {'tables':tables,'geos':geos}
                      
    def build_generate_rows(self, state):
        '''A generator that yields rows from the state geo files. It will 
        unpack the fixed width file and return a dict'''
        import struct
        import zipfile

        table = self.schema.table('geofile')
        header, unpack_str, length = table.get_fixed_unpack() #@UnusedVariable    

        rows = 0

        def test_zip_file(f):
            try:
                with zipfile.ZipFile(f) as zf:
                    return zf.testzip() is None
            except zipfile.BadZipfile:
                return False

        geo_source = self.urls['geos'][state]
    
        geo_zip_file = self.filesystem.download(geo_source, test_zip_file)

        grf = self.filesystem.unzip(geo_zip_file)

        geofile = open(grf, 'rbU', buffering=1*1024*1024)

        for line in geofile.readlines():
            
            rows  += 1
            
            if rows > 20000 and self.run_args.test:
                break

            try:
                geo = struct.unpack(unpack_str, line[:-1])
            except struct.error as e:
                self.error("Struct error for state={}, file={}, line_len={}, row={}, \nline={}"
                           .format(state,grf,len(line),rows, line))
             
            if not geo:
                raise ValueError("Failed to match regex on line: "+line) 

            yield dict(zip(header,geo))

        geofile.close()


class Us2000CensusFactBundle(UsCensusFactBundle):
    '''
    Bundle code for US 2000 Census, Summary File 1
    '''

    def __init__(self,directory=None):
        
        
        self.super_ = super(Us2000CensusFactBundle, self)
        self.super_.__init__(directory)
        
        
    def _scrape_urls(self, rootUrl, states_file, suffix='_uf1'):
        '''Extract all of the URLS from the Census website and store them'''
        import urllib
        import urlparse
        import re
        from bs4 import BeautifulSoup
    
        log = self.log
        tick = self.ptick
    
        # Load in a list of states, so we know which links to follow
        with open(states_file) as f:
            states = map(lambda s: s.strip(),f.readlines())
        
        # Root URL for downloading files. 
       
        doc = urllib.urlretrieve(rootUrl)
        
        log('Getting URLS from '+rootUrl)
        # Get all of the links
        log('S = state, T = segment table, g = geo')
        tables = {}
        geos = {}

        with open(doc[0]) as df:
            for link in BeautifulSoup(df).find_all('a'):
                tick('S')
                if not link.get('href') or not link.string or not link.contents:
                    continue# Didn't get a sensible link
                # Only descend into links that name a state
                state = link.get('href').strip('/')
              
                if link.string and link.contents[0] and state in states :
                    stateUrl = urlparse.urljoin(rootUrl, link.get('href'))
                    stateIndex = urllib.urlretrieve(stateUrl)
                    # Get all of the zip files in the directory
                    
                    with open(stateIndex[0]) as f:
                    
                        for link in  BeautifulSoup(f).find_all('a'):
                            if link.get('href') and  '.zip' in link.get('href'):
                                final_url = urlparse.urljoin(stateUrl, link.get('href')).encode('ascii', 'ignore')
                               
                                
                                if 'geo'+suffix in final_url:
                                    tick('g')
                                    state = re.match('.*/(\w{2})geo'+suffix, final_url).group(1)
                                    geos[state] = final_url
                                else:
                                    tick('T')
                                    res = '.*/(\w{2})(\d{5})'+suffix
                                    m = re.match(res, final_url)
        
                                    if not m:
                                        raise Exception("Failed to match {} to {} ".format(res, final_url))
        
                                    state,segment = m.groups()
                                    segment = int(segment.lstrip('0'))
                                    if not state in tables:
                                        tables[state] = {}
                                        
                                    tables[state][segment] = final_url
            
        return {'tables':tables,'geos':geos}

    def generate_schema_rows(self):
        '''This generator yields schema rows from the schema defineition
        files. This one is specific to the files produced by dumpoing the Access97
        shell for the 2000 census '''
        import csv

        with open(self.headers_file, 'rbU') as f:
            reader  = csv.DictReader(f )
            last_seg = None
            table = None
            for row in reader:

                if not row['TABLE']:
                    continue
                
                if row['SEG'] and row['SEG'] != last_seg:
                    last_seg = row['SEG']
             
                text = row['TEXT'].decode('utf8','ignore').strip()
    
                # The first two rows for the table give information about the title
                # and population universe, but don't have any column info. 
                if( not row['FIELDNUM'] or row['FIELDNUM'] == 'A' ):
                    if  row['TABNO']:
                        table = {'type': 'table', 
                                 'name':row['TABLE'],'description':text
                                 }
                    else:
                        table['universe'] = text.replace('Universe:','').strip()  
                else:
                    
                    # The whole table will exist in one segment ( file number ) 
                    # but the segment id is not included on the same lines ast the
                    # table name. 
                    if table:
                        # This is yielded  here so we can get the segment number. 
                        table['segment'] = row['SEG'] 
                        table['data'] = {'segment':row['SEG'], 'fact':True}
                        yield table
                        table  = None
                        
                    col_pos = int(row['FIELDNUM'][-3:])
                    
                    yield {
                           'type':'column','name':row['FIELDNUM'], 
                           'description':text.strip(),
                           'segment':int(row['SEG']),
                           'col_pos':col_pos,
                           'decimal':int(row['DECIMAL'])
                           }
                
    def build_generate_seg_rows(self, seg_number, source):
        '''Generate rows for a segment file. Call this generator with send(), 
        passing in the lexpected logrecno. If the next row does not have that 
        value, return a blank row until the logrecno values match. '''
        import csv, io, zipfile
        next_logrecno = None
        l = 0
        
        def test_zip_file(f):
            try:
                with zipfile.ZipFile(f) as zf:
                    return zf.testzip() is None
            except zipfile.BadZipfile:
                return False

        zip_file = self.filesystem.download(source, test_zip_file)
        rf = self.filesystem.unzip(zip_file)
        of = open(rf, 'rbU', buffering=1*1024*1024)
        
        for row in csv.reader(of):
            l += 1
            # The next_logrec bit takes care of a differece in the
            # segment files -- the PCT tables to not have entries for
            # tracts, so there are gaps in the logrecno sequence for those files. 
            while next_logrecno is not None and next_logrecno != row[4]:
                next_logrecno = (yield seg_number,  [])
     
            next_logrecno = (yield seg_number,  row)
   
        of.close()
                 
        if l == 0:
            raise RuntimeError("Didn't get any lines from {} ".format(zip_file))
                 
        return
 
    def build_generate_rows(self, state, geodim=False):
        '''A Generator that yelds a tuple that has the logrecno row
        for all of the segment files and the geo file. '''
        import struct

        table = self.schema.table('geofile')
        header, unpack_str, length = table.get_fixed_unpack() #@UnusedVariable
         
        geo_source = self.urls['geos'][state]
      
        gens = [self.build_generate_seg_rows(n,source) for n,source in self.urls['tables'][state].items() ]

        geodim_gen = self.build_generate_geodim_rows(state) if geodim else None
     
        rows = 0

        def test_zip_file(f):
            import zipfile
            try:
                with zipfile.ZipFile(f) as zf:
                    return zf.testzip() is None
            except zipfile.BadZipfile:
                return False

        geo_zip_file = self.filesystem.download(geo_source, test_zip_file)
        grf = self.filesystem.unzip(geo_zip_file)
        geofile = open(grf, 'rbU', buffering=1*1024*1024)

        first = True
        for line in geofile.readlines():
            
            rows  += 1
            
            if rows > 20000 and self.run_args.test:
                break

            try:
                geo = struct.unpack(unpack_str, line[:-1])
            except struct.error as e:
                self.error("Struct error for state={}, file={}, line_len={}, row={}, \nline={}"
                           .format(state,grf,len(line),rows, line))
                raise e
             
            if not geo:
                raise ValueError("Failed to match regex on line: "+line) 

            segments = {}
    
            lrn = geo[6]
       
            # load segment data from all of the files. 
            for index, g in enumerate(gens):
                try:
                    seg_number,  row = g.send(None if first else lrn)
                    segments[seg_number] = row
                    # The logrecno must match up across all files, except
                    # when ( in PCT tables ) there is no entry
                    if len(row) > 5 and row[4] != lrn:
                        raise Exception("Logrecno mismatch for seg {} : {} != {}"
                                        .format(seg_number, row[4],lrn))
                except StopIteration:
                    # Apparently, the StopIteration exception, raised in
                    # a generator function, gets propagated all the way up, 
                    # ending all higher level generators. thanks for nuthin. 
                    
                    #self.log("Got StopIteration in build_generate_rows at logrec={}. Is seg file state={} index={} seg_number={} shorter?"
                    #         .format(lrn,state, index, seg_number))
                    
                    break
 
            geodim = geodim_gen.next() if geodim_gen is not None else None

            if geodim and geodim[0] != int(lrn):
                m = "Logrecno mismatch for geodim : {} != {}".format(geodim[0],lrn)
                self.error(m)
                raise Exception(m)
            
            first = False
            
            if not 1 in segments:
                # There are segments that are shorter than others ( There are two groups
                # of sizes, but the first segment is always the same size ( in lines ) 
                # as the geo file. If not, it is an error. 
                m = "Segment 1 is short for state={}".format(state)
                self.error(m)
                raise Exception(m)
                
            yield state, segments[1][4], dict(zip(header,geo)), segments, geodim

        geofile.close()
