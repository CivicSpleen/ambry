'''
Created on Feb 12, 2013

@author: eric
'''

from ambry.identity import PartitionIdentity
from osgeo import gdal, gdal_array, osr
from osgeo.gdalconst import GDT_Float32, GDT_Byte, GDT_Int16
from numpy  import *
  

class DensityImage(object):
    '''
    classdocs
    '''

    def info(self):
        from numpy import histogram
        
        print 'OFFSETS       :',self.x_offset_d, self.y_offset_d
        print "size          :",self.x_size, self.y_size
        print 'UL Corner     :',self.bb.min_x, self.bb.max_y
        print 'LR Corner     :',self.bb.max_x, self.bb.min_y
        print "Value min, max:",self.a.min(),self.a.max()
        print "Elem Average  :",average(self.a)
        print "Masked Average:",ma.average(self.a)
        print "Mean          :",self.a.mean()
        print "Median        :",median(self.a)
        print "Std Dev       :",std(self.a)
        try:
            print "Histogram     :", histogram(self.a.compressed())[0].ravel().tolist()
        except:
            print "Histogram     :", histogram(self.a)[0].ravel().tolist()
       
    def add_count(self, x_in,y_in,v=1):
        x = int(x_in*self.bin_scale) - self.x_offset_c 
        y = int(y_in*self.bin_scale) - self.y_offset_c 
        
        x -= 1
      
        self.a[y,x] += v

    def mask(self):
        masked = ma.masked_equal(self.a,0)  
        self.a = masked
            
    def std_norm(self):
        """Normalize to +-4 sigma on the range 0 to 1"""

        mean = self.a.mean()
        std = self.a.std()
        self.a = (( self.a - mean) / std).clip(-4,4) # Def of z-score
        self.a += 4
        self.a /= 8
        
        try:
            self.a.set_fill_value(0)
        except AttributeError:
            # If it isn't a masked array
            pass


    def unity_norm(self):
        """scale to the range 0 to 1"""

        range  = self.a.max() - self.a.min()
        self.a = (self.a - self.a.min()) / range
   
        try:
            self.a.set_fill_value(0)
        except AttributeError:
            # If it isn't a masked array
            pass

  
        
    def quantize(self, bins):
        from util import jenks_breaks
        from numpy.random import choice #@UnresolvedImport
        
        print "Uniquing"
        unique_ = choice(unique(self.a), 1000)
        print "Uniques: ", unique_.size
        print "Breaking"
        breaks = jenks_breaks(unique_, bins)
        
        print "Breaks",breaks
        
        digitized = digitize(self.a.ravel(), breaks)
        
        self.a = ma.array(reshape(digitized, self.a.shape), mask=self.a.mask)

