"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

import os

from . import DatabaseInterface
from .old.hdf5 import Hdf5File


class HdfDb(Hdf5File, DatabaseInterface):
    
    EXTENSION = '.hdf'
    
    def __init__(self, partition):
        self.partition = partition
        self.bundle = partition.bundle

        self.container = self.partition

        dir_ = os.path.dirname(self.path)
        if not os.path.exists(dir_):
            os.makedirs(dir_)

        super(HdfDb, self).__init__(self.path)  
   
    @classmethod
    def make_path(cls, container):
        return container.path + cls.EXTENSION

    @property 
    def path(self):
        return self.make_path(self.container)

    @property
    def md5(self):
        from ambry.util import md5_for_file
        return md5_for_file(self.path)

    def is_empty(self):
        # If the file is open, it will exist, so we need to check for stuff inside. 
        return not self.keys()
   
    def add_post_create(self, f):
        pass

    def close(self):
        pass  # Need ot figure out how to do this ...
