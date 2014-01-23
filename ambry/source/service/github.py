'''
Created on Sep 26, 2013

@author: eric
'''

from . import ServiceInterface,GitServiceMarker #@UnresolvedImport

class GitHubService(ServiceInterface,GitServiceMarker):
    
    def __init__(self, user, password, org=None, **kwargs):
        self.org = org
        self.user = user
        self.password = password
        self.ident_url = 'https://github.com/'
        self.url = ur = 'https://api.github.com/'
        
        self.urls ={ 
                    'repos' : ur+'orgs/{}/repos?page={{page}}'.format(self.org) if self.org else ur+'users/{}/repos'.format(self.user), 
                    'deleterepo' : ur+'repos/{}/{{name}}'.format(self.org if self.org else self.user),
                    'info' : ur+'repos/{}/{{name}}'.format(self.org),
                    'repogit' : ur+'{}/{{name}}.git'.format(self.org),
                    'yaml' : "https://raw.github.com/{}/{{name}}/master/bundle.yaml".format(self.org)
                    }
        
        self.auth = (self.user, self.password)
 
    def get(self, url):
        '''Constructs a request, using auth is the user is set '''
        import requests, json
        
        if self.user:
            r = requests.get(url, auth=self.auth)
        else:
            r = requests.get(url)
             
        return r
             
    def has(self,name):
        import requests, json

        url = self.urls['info'].format(name=name)

        r = self.get(url)
            
        if r.status_code != 200:
            return False
        else:
            return True

 
    def create(self, name):
        '''Create a new upstream repository'''
        import requests, json
        
        payload = json.dumps({'name':name})
        r = requests.post(self.urls['repos'], data=payload, auth=self.auth)
        if r.status_code >= 300:
            raise Exception(r.headers)
            
        else:
            return r.json()
    
    def delete(self, name):
        '''Delete the upstream repository'''
        import requests, json

        r = requests.delete(self.urls['deleterepo'].format(name=name), auth=self.auth)

        if r.status_code != 204:
            raise Exception(r.headers)
            
        else:
            return True
    
    
    def list(self):
        import requests, yaml
        from ambry.util import OrderedDictYAMLLoader
        import pprint
        from yaml.scanner import ScannerError
        
        out = []

        for page in range(1,500):
            url = self.urls['repos'].format(page=page)
            
            r = self.get(url)

            r.raise_for_status()

            for i,e in enumerate(r.json()): 
                url = e['url'].replace('api.github.com/repos', 'raw.github.com')+'/master/bundle.yaml'
                r = requests.get(url)
                r.raise_for_status()
                try:
                    config = yaml.load(r.content, OrderedDictYAMLLoader)
                except ScannerError:
                    print r.content
                    print '!!!',url
                    raise 
                ident = dict(config['identity'])
                ident['clone_url'] = e['clone_url']
                out.append(ident)

            if i < 29: # WTF is this? Page limit?
                break
            

        return out
  
    def repo_url(self, name):
        
        return self.urls['repogit'].format(name=name).replace('api.github','github')
    
    @property
    def ident(self):
        '''Return an identifier for this service'''
        from urlparse import urlparse, urlunparse
        parts = list(urlparse(self.ident_url)[:]) # convert to normal tuple
        
        u = self.org if self.org else self.user
        
        parts[2] = parts[2]+u

        return urlunparse(parts)
         
    def __str__(self):
        return "<GitHubService: user={} org={}>".format(self.user,self.org)
        