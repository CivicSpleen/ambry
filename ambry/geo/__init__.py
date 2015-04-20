"""Classes and methos for working with GIS files, shapes and raster arrays. 
"""

# Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE.txt


from collections import  namedtuple

Point = namedtuple('Point', ['x', 'y'])

from ..dbexceptions import  RequirementError

# Just testing for gdal
try:

    from osgeo import gdal
except ImportError as e:
    raise RequirementError("Failed to import gdal: {}".format(str(e)))

#from analysisarea import *
from kernel import *
from util import *
from array import *
