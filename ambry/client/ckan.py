"""Client for CKAN data repositories


Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from ambry.run import get_runconfig
import ckanclient # sudo pip install git+https://github.com/okfn/ckanclient.git
import ambry.client.exceptions as Exceptions
import requests
import json

def new_ckan(config):
    from ambry.dbexceptions import ConfigurationError
  
    url = config.url
    key = config.key
  
    return Ckan(url, key)
           
class Ckan(object):
    '''
    classdocs
    '''

    def __init__(self, url, key):
        '''
        Constructor
        '''
        import re

        if not re.search("/\d$", url): # prefer version 2 of the api
            url += '/2'
            pass

        # Instantiate the CKAN client.
        self.url = url
        self.key = key
        
    @property
    def auth_headers(self):
        return {'Authorization': self.key,
                'X-CKAN-API-Key': self.key,
                'Content-Type': 'application/json; charset=utf-8'
                 }
    
    
    def translate_name(self,name):
        return name.lower().replace('.','_')
        
    
        
    def get_or_new_group(self,name):
        url = self.url+'/rest/group/{name}'
      
        try:
            r = requests.get(url.format(name=name.lower()))
            r.raise_for_status()
            
        except requests.exceptions.HTTPError:
            
            payload = {
                'name': name.lower(),
                'title': name,
                'description': name
                }
            
        
            r = requests.post(self.url+'/rest/group',
                              headers =  self.auth_headers,
                             data=json.dumps(payload))
            try:
                r.raise_for_status()
            except Exception as e:
                print r.content
                raise
   
        return r.json()
     
    #
    # Packages
    #
    
    def list_packages(self):
        r = requests.get(self.url+'/rest/package')
        r.raise_for_status()
        return r.json()    
    
    def entity_from_bundle(self, name,  bundle ):
        props = bundle.config.group('properties')

        if not name:
            name =  bundle.identity.name

        import datetime
        t = str(datetime.datetime.now())

        return  {
            'title':  (props.get('title',None)),
            'name': name,
            'author_email' : bundle.identity.creator,
            'author': props.get('author',None),
            'maintainer_email' : bundle.identity.creator,
            'maintainer': props.get('maintainer',None),
            'extras': {
                'bundle/type' : 'bundle',
                'bundle/source' : bundle.identity.source,
                'bundle/dataset' : bundle.identity.dataset,
                'bundle/subset' : bundle.identity.subset,
                'bundle/variation' : bundle.identity.variation,
                'bundle/revision' : bundle.identity.revision,
                'bundle/id' : bundle.identity.id_,
                'bundle/name' : bundle.identity.name
            },
                          
            'version':  bundle.identity.revision,
            'homepage':  props.get('homepage',None),
            'url':  props.get('url',None),
            'notes':  props.get('notes',None),
            'url':  props.get('url',None),
            'tags':  props.get('tags',None),
            
        }       
    
    def merge_dict(self,old, new, recurse=True):
        
        out = {}
        
        old_extras = old.get('extras', {})
        if len(old_extras): del old['extras'] 
        new_extras = new.get('extras', {})
        if len(new_extras): del new['extras']    

        # Copy over the new items
        for k,v in new.items():
            if v is None:
                pass
            else:
                out[k] = v
        
        # copy over the old items that don't already exist
        for k,v in old.items():
            if v is not None and not out.get(k,False):
                out[k] = v
             
        if recurse:
            out['extras'] = self.merge_dict(old_extras, new_extras, False)
                        
        return out
    
    def get_package(self, id_):
        r = requests.get(self.url+'/rest/package/{id}'.format(id=id_),
                          headers =  self.auth_headers)
        try:
            r.raise_for_status()
            return r.json()  
        except Exception as e:
            print "ERROR: "+r.content
            raise e
    
    
    def put_package(self, pe):
        
        data = json.dumps(pe)
        url = self.url+'/rest/package/{id}'.format(id=pe['id'])
        
        r = requests.put(url, headers =  self.auth_headers, data = data )
        try:
            r.raise_for_status()
            return r.json()  
        except Exception as e:
            print "ERROR: "+r.content
            raise e  
    
    def update_or_new_bundle(self, bundle, type='bundle',  name=None, 
                             title=None, group_names=None, **kwargs):
        '''Create a new package for a bundle.'''
        import datetime

        if name is None:
            name = self.translate_name(bundle.identity.name)
        else:
            name = self.translate_name(name)

        if not group_names:  
            group_names = ['bundles']

        groups = [self.get_or_new_group(group_name) for group_name in group_names]

        try:
            r = requests.get(self.url+'/rest/package/{name}'.format(name=name))
            r.raise_for_status()
            
        except requests.exceptions.HTTPError:
            # Create minimal package, since we always update next. 
            
            if not bundle.config.about.get('organization',None):
                raise Exception("Bundle must specify an organization")
            
            payload = {'name': name,
                       'owner_org': bundle.config.about.get('organization',None)
                       }
            
            url  =self.url+'/rest/package'
            
            r = requests.post( url,
                              headers =  self.auth_headers,
                              data=json.dumps(payload))
            try:
                r.raise_for_status()
            except Exception as e:
                print "URL {}".format(url)
                print "REQUEST: {}".format(json.dumps(payload))
                print "RESPONSE: {}".format(r.content)
                raise e
   
        new_payload = self.entity_from_bundle(name, bundle)
     
        payload = self.merge_dict(r.json(), new_payload)

        if title is None:
            title = bundle.config.about.title.format(
                datetime=datetime.datetime.now().isoformat('T'),
                date=datetime.date.today().isoformat()
            )

        tables = {}
        
        for table in bundle.schema.tables:
            tables[table.name] = bundle.schema.as_text(table.name)

        from collections import  namedtuple

        Tables = namedtuple('Tables', tables.keys())

        tables = Tables(**tables)


        description = bundle.config.about.get('description','').format(
                datetime=datetime.datetime.now().isoformat('T'),
                date=datetime.date.today().isoformat(),
                bundle_name = bundle.identity.name,
                tables=tables,
                **(dict(bundle.config.about))
            )

        payload['notes'] = description
        payload['title'] = title
        payload['groups'] = [group['id'] for group in groups]
        payload['license_id'] = bundle.config.about.get('license','other')

        r = requests.post(self.url+'/rest/package/{name}'.format(name=name),
                          headers =  self.auth_headers,
                          data=json.dumps(payload))


        try:
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print r.content
            raise e
    


    def update_or_new_bundle_extract(self, bundle, name=None, **kwargs):    

        if name is None:
            name = self.translate_name(bundle.identity.name+'-extract')
        else:
            name = self.translate_name(name)
        
        group_names = kwargs.get('group_names',[])
       
        for group_name in bundle.config.group('about').get('groups',[]):
            group_names.append(group_name)
       
        return self.update_or_new_bundle(bundle,  name=name,  group_names=group_names, **kwargs)

    def delete_package(self, id_):
        
        url = self.url+'/rest/package/{id}'.format(id=id_)

        r = requests.delete(url,headers =  self.auth_headers)
                      
        r.raise_for_status()
        
        return

    def upload_file(self,file_path, name=None):
        """Upload a file to the repository and return the URL, or an exception on 
        errors"""
        from datetime import datetime
        import os
        import urlparse
        import re
        
        # see ckan/public/application.js:makeUploadKey for why the file_key
        # is derived this way.
        ts = datetime.isoformat(datetime.now()).replace(':','').split('.')[0]

        if name is None:
            name = os.path.basename(file_path)
        
        norm_name  = name.replace(' ', '-')
        file_key = os.path.join(ts, norm_name)
        
        # Inexplicably, this URL can't have the version number
        url = re.sub('\/\d$','', self.url)+'/storage/auth/form/{}'.format(file_key.strip('/'))

        r = requests.get(url,headers =  self.auth_headers)
        url_path = r.json()['action']
        
        files = [('file', os.path.basename(file_key), open(file_path).read())]
        fields = [('key', file_key)]
        content_type, body = self._encode_multipart_formdata(fields, files)

        headers= self.auth_headers
        headers['Content-Type'] = content_type
        headers['Content-Length'] = str(len(body))
        
        # And this one not only doesn't have the api version, it also doesn't have
        # 'api'
        netloc = urlparse.urlparse(self.url).netloc
        url = 'http://'+ netloc+ url_path
   
        r = requests.post(url,headers = headers,data=body)
        try:
            r.raise_for_status()
        except:
            print 'ERROR for url: {}'.format(url)
            print r.content
            raise
          
        return '%s/storage/f/%s' % (re.sub('/api\/\d$','', self.url), file_key)
     

    def md5_for_file(self, file_, block_size=2**20):
        '''Compute the MD5 for a file without taking up too much memory'''
        import hashlib
        md5 = hashlib.md5()
        with open(file_, 'r') as f:
            while True:
                data = f.read(block_size)
                if not data:
                    break
                md5.update(data)
        return md5.hexdigest()

    def add_file_resource(self, 
                          pe, # Parent package, as a dict
                          file_path, 
                          name, 
                          resource_type = 'data',
                          content_type=None,
                          **kwargs):

        import os
        import mimetypes

        package_url = self.url+'/rest/package/{id}'.format(id=pe['id'])

        # Fetch the pe again, in case the one passed in was incomplete. 
        r = requests.get(package_url,headers =  self.auth_headers)
        r.raise_for_status()

        pe2 = r.json()

        md5 = self.md5_for_file(file_path)
      

        if content_type is None:
            content_type = mimetypes.guess_type(file_path)[0] or 'application/octet-stream'

        st = os.stat(file_path)
        
        extant = None
        for r in pe2['resources']:
            if r['hash'] == md5:
                extant = r
        
        if extant:
            server_url = extant['url']
        else:
            server_url = self.upload_file(file_path, name=name) 

        resource = dict(name=name,
                mimetype=content_type,
                hash=md5,
                size=st.st_size, 
                url=server_url)
        
        for k,v in kwargs.items():
            if v is not None:
                resource[k] = v

        # Don't duplicate records with the same hash 
        pe2['resources'] = ( [ r for r in pe2['resources'] if r['hash'] != md5  and r['name'] != name] 
                             + [resource] )


        r = requests.put(package_url,
                  headers =  self.auth_headers,
                  data=json.dumps(pe2))
        r.raise_for_status()

        return r.json()
    
    def add_url_resource(self, pe, url, name, **kwargs):

        import os
        import mimetypes

        r = requests.head(url)
        size = r.headers.get('content-length',None)
        content_type = r.headers.get('content-type',None)

        # Fetch the pe again, in case the one passed in was incomplete. 
        package_url = self.url+'/rest/package/{id}'.format(id=pe['id'])
        r = requests.get(package_url,headers =  self.auth_headers)
        r.raise_for_status()
        pe2 = r.json()

        resource = dict(name=name,
                mimetype=content_type,
                size=size, 
                hash=kwargs.get('hash', None),
                url=url
                )
        
        for k,v in kwargs.items():
            if v is not None:
                resource[k] = v

        pe2['resources'] =  ([ r for r in pe2['resources'] if r['url'] != url ] 
                             + [resource])

        r = requests.put(package_url,
                  headers =  self.auth_headers,
                  data=json.dumps(pe2))
        r.raise_for_status()

        return r.json()    
    def submit_bundle(self):
        pass
    
    def submit_partition(self, bunde_ref):
        pass
    
    def _encode_multipart_formdata(self, fields, files):
        '''Encode fields and files to be posted as multipart/form-data.

        Taken from
        http://code.activestate.com/recipes/146306-http-client-to-post-using-multipartform-data/

        :param fields: a sequence of (name, value) tuples for the regular
            form fields to be encoded
        :param files: a sequence of (name, filename, value) tuples for the data
            to be uploaded as files

        :returns: (content_type, body) ready for httplib.HTTP instance

        '''
        import mimetypes
         
        
        BOUNDARY = '----------ThIs_Is_tHe_bouNdaRY_$'
        CRLF = '\r\n'
        L = []
        for (key, value) in fields:
            L.append('--' + BOUNDARY)
            L.append('Content-Disposition: form-data; name="%s"' % key)
            L.append('')
            L.append(value)
        for (key, filename, value) in files:
            
            content_type = mimetypes.guess_type(filename)[0] or 'application/octet-stream'
            
            L.append('--' + BOUNDARY)
            L.append('Content-Disposition: form-data; name="%s"; filename="%s"' % (key, filename))
            L.append('Content-Type: %s' % content_type)
            L.append('')
            L.append(value)
        L.append('--' + BOUNDARY + '--')
        L.append('')
        body = CRLF.join(L)
        content_type = 'multipart/form-data; boundary=%s' % BOUNDARY
        return content_type, body
