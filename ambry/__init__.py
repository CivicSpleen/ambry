"""Support for creating packages of data.

Ambry creates packages of data that simplify the process of finding,
cleaning, transforming and loading popular datasets. The data bundle format,
tools and management processes are designed to make common public data sets easy
to use and share, while allowing users to audit how the data they use has been
acquired and processed. The Data Bundle concept includes the data format, a
definition for bundle configuration and meta data, tools for manipulating
bundles, and a process for acquiring, processing and managing data. The goal of
a data bundle is for data analysts to be able to run few simple commands to find
a dataset and load it into a relational database.

Visit  http://ambry.io for more information.


Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt

"""

from ._meta import *

from ambry.util import memoize


@memoize
def config(path=None, root=None, db=None):
    """Return the default run_config object for this installation."""
    import ambry.run
    return ambry.run.load(path=path, root=root, db=db)


@memoize
def get_library(path=None, root=None, db=None):
    import ambry.library as _l
    """Return the default library for this installation."""

    rc = config(path=path, root=root, db=db )

    return _l.new_library(rc)