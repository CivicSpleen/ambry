"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from ..dbexceptions import DependencyError
from relational import RelationalWarehouse #@UnresolvedImport
from ..library import LibraryDb


class RedshiftWarehouse(RelationalWarehouse):
    
    def __init__(self, config,  resolver_cb = None):
        super(RedshiftWarehouse, self).__init__(config, resolver_cb) 