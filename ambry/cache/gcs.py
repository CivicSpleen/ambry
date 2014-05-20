"""
Cache interface for Google Cloud Store


Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from .  import Cache
from s3 import S3Cache
from remote import RemoteMarker
from ..util import copy_file_or_flo, get_logger
import os

global_logger = get_logger(__name__)

#logger.setLevel(logging.DEBUG) 

class GcsCache(S3Cache):
    '''A cache that transfers files to and from an S3 bucket
    
     '''

    def __init__(self, bucket=None, prefix=None, account=None, upstream=None,**kwargs):
        '''Init a new S3Cache Cache

        '''
        from boto.gs.connection import GSConnection

        super(S3Cache, self).__init__(upstream=upstream) # Skip parent __init__

        self.is_remote = False
        self.access_key = account['access']
        self.secret = account['secret']
        self.project = account['project']
        self.bucket_name = bucket
        self.prefix = prefix

        self.conn = GSConnection(self.access_key, self.secret, is_secure = False )
        self.bucket = self.conn.get_bucket(self.bucket_name)
        

    def __repr__(self):
        return "GcsCache: bucket={} prefix={} access={} ".format(self.bucket, self.prefix, self.access_key, self.upstream)
       

    
    
