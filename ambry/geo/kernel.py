"""Kernels are arrays used in 2D colvolution on analysis area arrays. 

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

import numpy as np

class OutOfBounds(Exception): pass

class Kernel(object):

    def __init__(self, size, matrix=None):
        self.size = size
        
        if size%2 == 0:
            raise ValueError("Size must be odd")
        
        # For 1-based array indexing, we'd have to +1, but this is zero-based
         
        if size > 1:
            self.center =  int(size/2) 
        else:
            self.center = 1
            
        self.offset = (self.size - 1) / 2 
         
        if matrix is None:               
            self.matrix = np.ones((self.size, self.size))
        else:
            self.matrix = matrix
            
        self._dindices = None

    def limit(self):
        '''Make the matrix spot sort of round, by masking values in the corners'''
        
        # Get the max value for the edge of the enclosed circle. 
        # This assumes that there is a radial gradient. 
        row_max = self.matrix[self.center][0]

        for (y_m,x_m), value in np.ndenumerate(self.matrix):
            if self.matrix[y_m][x_m] > row_max:
                self.matrix[y_m][x_m] = 0
                
    def round(self):
        '''Make the matrix spot sort of round, using a radius'''
        import math
        # Get the max value for the edge of the enclosed circle. 
        # This assumes that there is a radial gradient. 
        
        for (x_m,y_m), value in np.ndenumerate(self.matrix):
            if math.sqrt( (x_m-self.center)**2 + (y_m-self.center)**2 ) > float(self.size) / 2.:
                self.matrix[y_m][x_m] = 0

        
    def norm(self):
        #self.matrix /= sum(self.matrix)
        self.matrix /= self.matrix.max()
        
        
    def invert(self):
        ''''Invert the values, so the cells closer to the center 
        have the higher values. '''
        #range = self.matrix.max() - self.matrix.min() 

        self.matrix = self.matrix.max() - self.matrix
        self.inverted = ~self.inverted
       
    def quantize(self, bins=255):
        from util import jenks_breaks
        hist, edges = np.histogram(self.matrix.compressed(),bins=bins)
      
        print "Hist", hist
        print "Edges",edges
        
        breaks = jenks_breaks(self.matrix.compressed().tolist(), bins)
        print "Breaks",breaks
        
        l = list(set(self.matrix.compressed().tolist()))
        l.sort()
        print "Uniques", l
        
        print self.matrix.compressed()
        digits = np.digitize(self.matrix.ravel(), breaks)
        
        print self.matrix.size
        print digits.size
        print self.matrix.shape[0]
        
        s = np.ma.array(np.reshape(digits, self.matrix.shape), mask=self.matrix.mask)
        
        print s
      

    def bounds(self, a, point):
        
        y_max, x_max = a.shape
        m = None
        use_m=False
        
        if point.x < self.offset:
            if point.x < 0:
                return (False, None, None, None, None)
            
            x_start = max(point.x - self.offset,0)
            x_end = point.x + self.offset +1  
            m = self.matrix[:,(self.offset-point.x):self.matrix.shape[1]]
            use_m=True
            
        elif point.x+self.offset+1 >  x_max :
            if point.x > x_max:
                return (False, None, None, None, None)
            
            x_start = point.x - self.offset
            x_end = min(point.x + self.offset+1, x_max)
            m = self.matrix[:,0:self.matrix.shape[1]+ (x_max-point.x-self.offset)-1]
            use_m=True
        else:
            x_start = point.x - self.offset
            x_end = point.x + self.offset+1
        
        sm = (m if use_m else self.matrix)
        
        if point.y < self.offset:
            if point.y < 0:
                return (False, None, None, None, None)
            
            y_start = max(point.y - self.offset,0)
            y_end = point.y + self.offset+1
            m = sm[(self.offset-point.y):sm.shape[0],:]
            use_m=True
        elif point.y+self.offset+1 >  y_max:
            if point.y > y_max:
                return (False, None, None, None, None)
            
            y_start = point.y - self.offset
            y_end = point.y + self.offset+1
            m = sm[0:sm.shape[0]+ (y_max-point.y-self.offset)-1,:]
            use_m=True
        else:
            y_start = point.y - self.offset
            y_end = point.y + self.offset+1    
            
        if m is None:
            m = self.matrix
            
        return ( m,  y_start, y_end, x_start, x_end)
        
    @property     
    def dindices(self):
        '''Return the indices of the matrix, sorted by distance from the center'''
        import math
        
        if self._dindices is None:
            indices = []
            c = self.center
    
            for i, v in np.ndenumerate(self.matrix):
                indices.append(i)
                
            self._dindices = sorted(indices, key=lambda i: math.sqrt( (i[0]-c)**2 + (i[1]-c)**2 ))
             
        return self._dindices
         
    def diterate(self, a, point):
        '''Iterate over the distances from the point in the array a'''
        
        for i in self.dindices:
            x = point[0]+i[1]-self.center
            x = x if x >=0 else 0
            y = point[1]+i[0]-self.center
            y = y if y >=0 else 0
            yield i, (y,x )
        
        
               
    def apply(self,a, point, source = None,  f=None, v=None):
        """Apply the values in the kernel onto an array, centered at a point. 
        
        :param a: The array to apply to 
        :type a: numpy.array
        :param source: The source for reading data. Must have same dimensions as a
        :type a: numpy.array
        :param f: A two argument function that decides which value to apply to the array    
        :type f: callable
        :param point: The point, in the array coordinate system, where the center of the
        kernel will be applied
        :type point: Point
        :param v: External value to be passed into the function
        :type v: any
        """

        if v:
            from functools import partial
            f = partial(f,v)

        (m,  y_start, y_end, x_start, x_end) = self.bounds(a, point)

        if not source:
            source = a

        #print a.shape, point, x_start, x_end, y_start, y_end, (m if use_m else self.matrix).shape
        if m is not False: # 
            a[y_start:y_end, x_start:x_end] = f( source[y_start:y_end, x_start:x_end], m)
        else:
            raise OutOfBounds("Point {} is out of bounds for this array ( {} )".format(str(point), str(a.shape)))
                                          
                        
    def iterate(self, a, indices = None):
        '''Iterate over kernel sized arrays of the input array. If indices are specified, use them to iterate
        over some of the cells, rather than all of them. '''
        from ..geo import Point
        if indices is None:
            it = np.nditer(a,flags=['multi_index'] )
            while not it.finished:
                (m,  y_start, y_end, x_start, x_end) = self.bounds(a, Point(it.multi_index[1], it.multi_index[0]))
                yield  it.multi_index[0], it.multi_index[1], a[y_start:y_end, x_start:x_end], m
                it.iternext()     
        else:
            for y,x in zip(indices[0],indices[1]):
                (m,  y_start, y_end, x_start, x_end) = self.bounds(a, Point(x, y))
                yield y,x, a[y_start:y_end, x_start:x_end], m
        
    def apply_add(self,a,point,y=None):
        from ..geo import Point

        if y is not None:
            point = Point(point, y)
        return self.apply(a,point, f=lambda x,y: np.add(x,y))
    
    def apply_min(self,a,point):

        f = lambda a,b: np.where(a<b, a, b)
        return self.apply(a,point, f=f)        
    
    def apply_max(self,a,point):
  
        return self.apply(a,point, f=np.max)        
        
class ConstantKernel(Kernel):
    """A Kernel for a constant value"""
    
    def __init__(self, size=1, value = None ):
        
        super(ConstantKernel, self).__init__(size)
        
        self.value  = value
        
        if value:
            self.matrix = np.ones((size, size))*value
        else:
            self.matrix = np.ones((size, size))
            self.matrix /= sum(self.matrix) # Normalize the sum of all cells in the matrix to 1
            
        self.offset = (self.matrix.shape[0] - 1) / 2 
    
                  
         
class GaussianKernel(Kernel):
    
    def __init__(self, size=9, fwhm=3 ):
        
        super(GaussianKernel, self).__init__(size)

        m = self.makeGaussian(size, fwhm)

        self.offset = (m.shape[0] - 1) / 2 
        self.matrix = m

    @staticmethod
    def makeGaussian(size, fwhm = 3):
        """ Make a square gaussian kernel.
    
        size is the length of a side of the square
        fwhm is full-width-half-maximum, which
        can be thought of as an effective radius.
        """

        
        x = np.arange(0, size, 1, np.float32)
        y = x[:,np.newaxis]
        x0 = y0 = size // 2
        ar = np.array(np.exp(-4*np.log(2) * ((x-x0)**2 + (y-y0)**2) / fwhm**2))
        m =  np.ma.masked_less(ar, ar[0,x0+1]).filled(0) #mask less than the value at the edge to make it round. 

        m /= sum(m) # Normalize the sum of all cells in the matrix to 1
      
        return m
           
class DistanceKernel(Kernel):
    ''' Each cell is the distance, in cell widths, from the center '''
    def __init__(self, size): 
        
        import math
        
        super(DistanceKernel, self).__init__(size)

        self.inverted = False
        
        #self.matrix = ma.masked_array(zeros((size,size)), mask=True, dtype=float)
        self.matrix = np.zeros((size,size), dtype=float)
        
        row_max = size - self.center - 1 # Max value on a horix or vert edge
     
        for (y_m,x_m), value in np.ndenumerate(self.matrix):
                r  = np.sqrt( (y_m-self.center)**2 + (x_m-self.center)**2)
                self.matrix[y_m,x_m] = r
                    
class MostCommonKernel(ConstantKernel):
    """Applies the most common value in the kernel area"""
    
    def __init__(self, size=1):
        super(MostCommonKernel, self).__init__(size, 1)
        
    
    def apply(self,a, point, source = None,  f=None, v=None):
        """Apply the values in the kernel onto an array, centered at a point. 
        
        :param a: The array to apply to 
        :type a: numpy.array
        :param source: The source for reading data. Must have same dimensions as a
        :type a: numpy.array
        :param f: A two argument function that decides which value to apply to the array    
        :type f: callable
        :param point: The point, in the array coordinate system, where the center of the
        kernel will be applied
        :type point: Point
        :param v: External value to be passed into the function
        :type v: any
        """

        

        if v:
            from functools import partial
            f = partial(f,v)

        (m,  y_start, y_end, x_start, x_end) = self.bounds(a, point)

        if source is None:
            source = a

        d1 = np.ravel(source[y_start:y_end, x_start:x_end])

        bc = np.bincount(d1, minlength=10)
        am = np.argmax(bc)
        
        if am != a[point[0], point[1]]:
            print am


        a[y_start:y_end, x_start:x_end] = 1        
                    
class ArrayKernel(Kernel):
    '''Convert an arbitary ( hopefully small ) numpy array 
    into a kernel'''
    
    def __init__(self, a , const = None):
        
        y,x = a.shape
        
        size = max(x,y)
        
        if size % 2 == 0:
            size += 1
            
        pad_y = size - y
        pad_x = size - x
        
        b = np.pad(a,((0,pad_y),(0,pad_x)), 'constant', constant_values=((0,0),(0,0)))  # @UndefinedVariable
           
        if const:
            b *= const
              
        super(ArrayKernel, self).__init__(size, b) 

        # original shape. 
        self.oshape = a.shape
        
        
        
        