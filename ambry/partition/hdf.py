"""Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

  
from . import PartitionBase
from ..identity import PartitionIdentity, PartitionName
from ..database.hdf import HdfDb


class HdfPartitionName(PartitionName):
    PATH_EXTENSION = '.hdf'
    FORMAT = 'hdf'

class HdfPartitionIdentity(PartitionIdentity):
    _name_class = HdfPartitionName

class HdfPartition(PartitionBase):
    '''A Partition that hosts a Spatialite for geographic data'''

    _id_class = HdfPartitionIdentity
    _db_class = HdfDb
    
    def __init__(self, bundle, record, **kwargs):
        super(HdfPartition, self).__init__(bundle, record)


    @property
    def database(self):

        if self._database is None:
            self._database = HdfDb(self)
          
        return self._database

    def create(self):
        pass


    def __repr__(self):
        return "<hdf partition: {}>".format(self.name)