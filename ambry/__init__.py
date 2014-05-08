"""Support for creating packages of data

Ambry creates packages of data that simplify the process of finding,
cleaning, transforming and loading popular datasets. The data bundle format,
tools and management processes are designed to make common public data sets easy
to use and share, while allowing users to audit how the data they use has been
acquired and processed. The Data Bundle concept includes the data format, a
definition for bundle configuration and meta data, tools for manipulating
bundles, and a process for acquiring, processing and managing data. The goal of
a data bundle is for data analysts to be able to run few simple commands to find
a dataset and load it into a relational database.

Visit Visit http://ambry.io for more information.


Copyright (c) 2014 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

__author__ = "Eric Busboom"
__copyright__ = "Copyright (c) 2014 Clarinova"
__credits__ = []
__license__ = "Revised BSD"
__version__ = '0.3.255'
__maintainer__ = "Eric Busboom"
__email__ = "eric@clarinova.com"
__status__ = "Development"

from util import memoize
import ambry.library as _l
from ambry.bundle import new_analysis_bundle


@memoize
def config():
    '''Return the default run_config object for this installation'''
    from ambry.run import get_runconfig
    return get_runconfig()


@memoize
def library(name='default'):
    '''Return the default library for this installation'''
    return _l.new_library(config().library(name))


def ilibrary(name='default'):
    '''Return the default Analysislibrary for this installation, which is like the Library returned by
    library(), but configured for use in IPython'''
    return _l.AnalysisLibrary(library())
