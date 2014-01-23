'''
Created on Aug 29, 2013

@author: eric
'''


import gslib # pip: gsutil, Google cloud storage @UnresolvedImport

from oauth2client.file import Storage #@UnresolvedImport
from ..cache import new_cache
import os

class CacheCredentialStorage(Storage):
    
    def __init__(self, config, name='default'):
        
        self.cache = new_cache(config)

        self.path = self.cache.path('_credentials/'+name)
        
        if not os.path.isdir(os.path.dirname(self.path)):
            os.makedirs(os.path.dirname(self.path))
        
        super(CacheCredentialStorage, self).__init__(self.path)
    
class Schema(object):
    
    def __init__(self):
        self.fields = []
        
    def add_field(self, name, type, mode, fields=None):
        pass
        
    def __str__(self):
        import json
        
        return json.dumps({ 'fields': self.fields})
    
    

class Table(object):
    pass
    
class Job(object):
    pass
   
class JobReference(object):
    pass
   
    def __init__(self, project_id):
        pass
   
class LoadJob(Job):
    pass

    def __init__(self, schema):
        
        self.schema = schema
        self.load = {
                     'sourceUris' : [],
                     'destinationTable': None,
                     'projectId': None,
                     'datasetId': None,
                     'tableId': None,
                     }
        
class BigQuery(object):
    '''
    classdocs
    '''

    def __init__(self, account):
        '''
        Constructor
        '''
    
        self.account = account

        self.project = self.account['project']
        self._service = None

    def authorize_user(self):
            
        import gflags
        import httplib2
            
        from oauth2client.client import OAuth2WebServerFlow #@UnresolvedImport
        from oauth2client.tools import run #@UnresolvedImport
         
        FLAGS = gflags.FLAGS
        FLOW = OAuth2WebServerFlow(
            client_id=self.account['client_id'],
            client_secret=self.account['client_secret'],
            scope='https://www.googleapis.com/auth/bigquery',
            user_agent='ambry')
         
        gflags.DEFINE_enum('logging_level', 'ERROR',
            ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
            'Set the level of logging detail.')

        cred_store =  CacheCredentialStorage(self.account['store'])
 
        credentials = cred_store.get()
        
        if credentials is None or credentials.invalid:
            credentials = run(FLOW, cred_store)
         
        #httplib2.debuglevel = 1
        http = httplib2.Http()
        self.http = credentials.authorize(http)
         
        return self.http

    def authorize_server(self):
        import httplib2
        from apiclient.discovery import build #@UnresolvedImport
        from oauth2client.client import SignedJwtAssertionCredentials #@UnresolvedImport

        # REPLACE WITH THE SERVICE ACCOUNT EMAIL FROM GOOGLE DEV CONSOLE
        key_file = self.account['key_file']
              

        with file(key_file, 'rb') as f:
            key = f.read()

        credentials = SignedJwtAssertionCredentials(self.account['service_email'],key,
            scope='https://www.googleapis.com/auth/bigquery')
        
        http = httplib2.Http()
        self.http = credentials.authorize(http)
        
        return self.http
                
    @property
    def service(self):
        from apiclient.discovery import build #@UnresolvedImport
        
        if not self._service:

            self._service = build('bigquery', 'v2', http=self.http)        
        
        return self._service
        
      
    def list(self):
        
        datasets = self.service.datasets()
        response = datasets.list(projectId=self.project).execute(self.http)
        
        print('Dataset list:\n')
        for dataset in response['datasets']:
            print dataset
                      
      
    def put(self, rel_path):
        pass
    
    def get(self, rel_path):
        pass
           

