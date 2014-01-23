"""The Bundle object is the root object for a bundle, which includes acessors
for partitions, schema, and the filesystem

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

__author__ = 'eric'

from .bundle import *

def get_identity(path):
    '''Get an identity from a database, either a bundle or partition'''
    from ..database.sqlite import SqliteBundleDatabase #@UnresolvedImport

    raise Exception("Function deprecated")

    db = SqliteBundleDatabase(path)

    bdc = BundleDbConfig(db)

    type_ = bdc.get_value('info','type')

    if type_ == 'bundle':
        return  bdc.dataset.identity
    elif type_ == 'partition':
        return  bdc.partition.identity
    else:
        raise Exception("Invalid type: {}", type)
