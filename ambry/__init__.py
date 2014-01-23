"""Support for creating packages of data

Data Bundles are packages of data that simplify the process of finding,
cleaning, transforming and loading popular datasets. The data bundle format,
tools and management processes are designed to make common public data sets easy
to use and share, while allowing users to audit how the data they use has been
acquired and processed. The Data Bundle concept includes the data format, a
definition for bundle configuration and meta data, tools for manipulating
bundles, and a process for acquiring, processing and managing data. The goal of
a data bundle is for data analysts to be able to run few simple commands to find
a dataset and load it into a relational database.

Visit Visit http://wiki.clarinova.com/display/CKDB/Data+Bundles for more 
information.


Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

__author__ = "Eric Busboom"
__copyright__ = "Copyright (c) 2013 Clarinova"
__credits__ = []
__license__ = "Revised BSD"
__version__ = '0.0.3'
__maintainer__ = "Eric Busboom"
__email__ = "eric@clarinova.com"
__status__ = "Development"

def resolve_id(arg, bundle=None, library=None):
    '''resolve any of the many ways of identifying a partition or
    bundle into an ObjectNumber for a Dataset or Partition '''
    from identity import ObjectNumber, Identity


    if isinstance(arg, basestring):
        on = ObjectNumber.parse(arg)
    elif isinstance(arg, ObjectNumber):
        return arg
    elif isinstance(arg, Identity):
        if not arg.id_ and bundle is None:
            raise Exception("Identity does not have an id_ defined")
        elif not arg.id_ and bundle is not None:
            raise NotImplementedError("Database lookup for Identity Id via bundle  is not yet implemented")
        elif not arg.id_ and bundle is not None:
            raise NotImplementedError("Database lookup for Identity Id via library is not yet implemented")
        else:
            on = ObjectNumber.parse(arg.id_)
 
    else:
        # hope that is has an identity field
        on = ObjectNumber.parse(arg.identity.id_)
        
    return on
