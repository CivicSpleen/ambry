"""Rest interface for accessing a remote library. 

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from ambry.client.siesta  import API
import ambry.client.exceptions
import requests
import json

class NotFound(Exception):
    pass

class RestError(Exception):
    pass

def raise_for_status(response):
    import pprint

    e = ambry.client.exceptions.get_http_exception(response.status)
        
    if e:
        raise e(response.message)

class RemoteLibrary(object):

    def __init__(self, url):
        '''
        '''

        self._url = url

        if not self._url[-1] == '/':
            self._url += '/'

        self.last_response = None

    def url(self,u,*args, **kwargs):

        if u[0] == '/':
            u = u[1:]

        return self._url+u.format(*args, **kwargs)



    def get(self, url, params={}):


        r = requests.get(url, params=params)

        self.handle_status(r)

        return self.handle_return(r)

    def put(self, url, params={}, data=None):

        if not isinstance(data,(list,dict)):
            data = [data]

        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

        r = requests.put(url, params=params, data=json.dumps(data), headers=headers)

        self.handle_status(r)

        return self.handle_return(r)

    def post(self, url, params={}, data=None):

        if not isinstance(data,(list,dict)):
            data = [data]

        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

        r = requests.post(url, params=params, data=json.dumps(data), headers=headers)

        self.handle_status(r)

        return self.handle_return(r)


    def handle_status(self, r):
        import exceptions

        if r.status_code >= 300:

            try:
                o = r.json()
            except:
                o = None

            if isinstance(o, dict) and 'exception' in o:
                e = self.handle_exception(o)
                raise e

            if 400 <= r.status_code < 500:
                raise exceptions.NotFound("Failed to find resource for URL: {}".format(r.url))

            r.raise_for_status()

    def handle_return(self, r):

        if r.headers.get('content-type', False) == 'application/json':
            self.last_response = r
            return r.json()
        else:
            return r

    def handle_exception(self, object):
        '''If self.object has an exception, re-construct the exception and
        return it, to be raised later'''

        import types, sys

        field = object['exception']['class']

        pre_message = ''
        try:
            class_ = getattr(sys.modules['ambry.client.exceptions'], field)
        except AttributeError:
            pre_message = "(Class: {}.) ".format(field)
            class_ = Exception

        if not isinstance(class_, (types.ClassType, types.TypeType)):
            pre_message = "(Class: {},) ".format(field)
            class_ = Exception


        args = object['exception']['args']

        # Add the pre-message, if the real exception type is not known.
        if isinstance(args, list) and len(args) > 0:
            args[0] = pre_message + str(args[0])

        # Add the trace
        try:
            if args:
                args[0] = args[0] + "\n---- Server Trace --- \n" + object['exception']['trace']
            else:
                args.append("\n---- Server Trace --- \n" + object['exception']['trace'])
        except Exception as e:
            print "Failed to augment exception. {}, {}".format(args, object)
            print 'AAA', e, e.message

        return  class_(*args)


    # @get('/')

    def get_root(self):
        return self.get(self.url("/"))

    # @get('/datasets/find/<term>')
    # @post('/datasets/find')

    # @get('/datasets')
    def list(self, datasets=None, location=None, key='vid'):
        '''List the identities of all of the datasets in the library '''
        from ..identity import Identity

        if not datasets:
            datasets = {}

        for cache_key, data in self.get(self.url("/datasets")).items():
            ident = Identity.from_dict(data['identity'])

            ck = getattr(ident, key)

            if ck not in datasets:
                datasets[ck] = ident
            else:
                ident = datasets[ck]

            ident.urls = data['urls']

        return datasets

    # @get('/resolve/<ref>')
    def resolve(self, ref, location = None):
        '''Returns an identity given a vid, name, vname, cache_key or object number'''
        from ..identity import Identity

        d =  self.get(self.url("/info/{}", ref))

        if not d:
            return None

        if d['response'] == 'partition':
            data = d['partitions'].values()[0]
        else:
            data = d

        ident_d = data['identity']

        ident =  Identity.from_dict(ident_d)
        ident.data = data['urls']

        if ident.is_bundle:
            return ident
        else:
            dsid = ident.as_dataset()
            dsid.add_partition(ident)

            return dsid

    # @get('/info/<ref>')
    def info(self, ref):
        '''Returns the server's information page for an object, given any kind of ref'''
        from ..identity import Identity

        return self.get(self.url("/info/{}", ref))

    def get_stream(self, ref):
        '''Return a FLO that streams the file associated with a the given reference'''

        info = self.info(ref)


        if info['response'] == 'dataset':
            url = info['urls']['db']
        else:
            url = info['partitions'].values()[0]['urls']['db']

        r = requests.get(url, verify=False, stream=True)

        if r.status_code != 200:

            from xml.dom import minidom
            from xml.parsers.expat import ExpatError
            try:

                o = minidom.parse(r.raw)
                # Assuming the response is in XML because we are usually calling s3
                raise RestError("{} Error from server after redirect  : XML={}"
                .format(r.status_code, o.toprettyxml()))
            except ExpatError:
                raise RestError("Failed to get {}, status = {}, content = {} "
                                .format(url, r.status_code, r.content))


        if r.headers.get('content-encoding','') == 'gzip':
            from ..util import FileLikeFromIter
            # In  the requests library, iter_content will auto-decompress
            response = FileLikeFromIter(r.iter_content(chunk_size=128 * 1024))
        else:
            response = r.raw


        return response

    # @get('/datasets/<did>')
    def dataset(self, vid):
        '''Get information about a dataset, including the identity and
        all of the partitions '''
        from ..identity import Identity

        out = []

        r = self.get(self.url("/datasets/{}", vid))
        ident = Identity.from_dict(r['identity'])

        for pvid, data in r['partitions'].items():
            pident = Identity.from_dict(data['identity'])
            pident.urls = data['urls']
            ident.add_partition(pident)

        return ident


    # @post('/datasets/<did>')
    def load_dataset(self,ident):

        r = self.post(self.url("/datasets/{}", ident.vid), data=ident.dict)

    def x_put(self, b_or_p):

        pass


    # @get('/datasets/<did>/csv')
    # @post('/datasets/<did>/partitions/<pid>')
    # @get('/datasets/<did>/db')
    # @get('/files/<key:path>')
    # @get('/key/<key:path>')
    # @get('/ref/<ref:path>')
    # @get('/datasets/<did>/<typ:re:schema\\.?.*>')
    # @get('/datasets/<did>/partitions/<pid>')
    # @get('/datasets/<did>/partitions/<pid>/db')
    # @get('/datasets/<did>/partitions/<pid>/tables')
    # @get('/datasets/<did>/partitions/<pid>/tables/<tid>/csv')
    # @get('/datasets/<did>/partitions/<pid>/tables/<tid>/csv/parts')
    # @get('/datasets/<did>/partitions/<pid>/csv')
    # @get('/datasets/<did>/partitions/<pid>/csv/parts')


    #
    # Testing
    #

    # @get('/test/echo/<arg>')
    def get_test_echo(self, term):
        return self.get(self.url("/test/echo/{}", term))[0]

    # @put('/test/echo')
    def put_test_echo(self, term):
        r =  self.put(self.url("/test/echo"), data=term)[0]

        if r:
            return r[0]
        else:
            return None


    # @get('/test/exception')
    def get_test_exception(self):
        return self.get(self.url("/test/exception"))


    # @put('/test/exception')
    # @get('/test/isdebug')
    def get_is_debug(self):
        return self.get(self.url("/test/isdebug"))

    # @post('/test/close')
    def post_close(self):
        return self.post(self.url("/test/close"))


class OldRestApi(object):
    '''Interface class for the Databundles Library REST API
    '''

    def __init__(self, url):
        '''
        '''
        
        self.url = url

    @property
    def remote(self):
        # It would make sense to cache self.remote = API(), but siesta saves the id
        # ( calls like remote.datasets(id).post() ), so we have to either alter siesta, 
        # or re-create it every call. 
        return API(self.url)


    def get_ref(self, id_or_name):
        '''Return a tuple of (rel_path, dataset_identity, partition_identity)
        for an id or name'''

        id_or_name = id_or_name.replace('/','|')

        response  = self.remote.ref(id_or_name).get()

        if response.status == 404:
            raise NotFound("Didn't find a file for {}".format(id_or_name))
        elif response.status != 200:
            raise RestError("Error from server: {} {}".format(response.status, response.reason))
        
        return response.object
  
    def _process_get_response(self, id_or_name, response, file_path=None, uncompress=False, cb=None):
        
        if response.status == 404:
            raise NotFound("Didn't find a file for {}".format(id_or_name))
        
        if response.status == 303 or response == 302:
            import requests

            location = response.get_header('location')

            r = requests.get(location, verify=False, stream=True)

            if r.status_code != 200:
                from xml.dom import minidom
                o = minidom.parse(r.raw)

                # Assuming the response is in XML because we are usually calling s3
                raise RestError("{} Error from server after redirect to {} : XML={}"
                                .format(r.status_code,location,  o.toprettyxml()))
                
            if r.headers['content-encoding'] == 'gzip':
                from ..util import FileLikeFromIter   
                 # In  the requests library, iter_content will auto-decompress
                response = FileLikeFromIter(r.iter_content())
            else:
                response = r.raw

        elif response.status != 200:
            raise RestError("Error from server: {} {}".format(response.status, response.reason))
        
        if file_path:
            
            if file_path is True:
                import uuid,tempfile,os
        
                file_path = os.path.join(tempfile.gettempdir(),'rest-downloads',str(uuid.uuid4()))
                if not os.path.exists(os.path.dirname(file_path)):
                    os.makedirs(os.path.dirname(file_path))  
               
            if uncompress:
                # Implement uncompression with zli, 
                # see http://pymotw.com/2/zlib/
                
                raise NotImplementedError()
               
            chunksize = 8192  
            i = 0
            
            with open(file_path,'w') as file_:
                
                chunk =  response.read(chunksize) #@UndefinedVariable
                while chunk:
                    i += 1
                    if cb:
                        cb(0,i*chunksize)
                    file_.write(chunk)
                    chunk =  response.read(chunksize) #@UndefinedVariable

            return file_path
        else:
            return response
           
    def get(self, did, pid=None):
        '''Get a bundle by name or id and either return a file object, or
        store it in the given file object
        
        Args:
            id_or_name 
            file_path A string or file object where the bundle data should be stored
                If not provided, the method returns a response object, from which the
                caller my read the body. If file_path is True, the method will generate
                a temporary filename. 
        
        return
        
        '''
        from ambry.util import bundle_file_type
        from urllib import quote_plus

        try: did = did.id_ # check if it is actually an Identity object
        except: pass

        if pid:
            response  = self.remote.datasets(did).partitions(pid).get()
        else:
            response  = self.remote.datasets(did).get()

        return response.object # self._process_get_response(id_or_name, response, file_path, uncompress, cb=cb)
    
    
    def get_stream_by_key(self, key, cb=None):
        '''Get a stream to to the remote file. 
        
        Queries the REST api to get the URL to the file, then fetches the file
        and returns a stream, wrapping it in decompression if required. '''
        
        import requests, urllib
        from ..util.flo import MetadataFlo
        
        r1  = self.remote.key(key).get()

        location = r1.get_header('location')

        if not location:
            raise_for_status(r1)


        r = requests.get(location, verify=False, stream=True)
              
        stream = r.raw
              
        if r.headers['content-encoding'] == 'gzip':
            from ambry.util.sgzip import GzipFile
            stream = GzipFile(stream)
        
        return MetadataFlo(stream,r.headers)


              
    def get_partition(self, d_id_or_name, p_id_or_name, file_path=None, uncompress=False, cb=False):
        '''Get a partition by name or id and either return a file object, or
        store it in the given file object
        
        Args:
            id_or_name 
            file_path A string or file object where the bundle data should be stored
                If not provided, the method returns a response object, from which the
                caller my read the body. If file_path is True, the method will generate
                a temporary filename. 
        
        return
        
        '''
        response  = self.remote.datasets(d_id_or_name).partitions(p_id_or_name).get()
        
        return self._process_get_response(p_id_or_name, response, file_path, uncompress, cb=cb)

                     
    def put(self, metadata):
        ''''''
        import json
        from ambry.identity import Identity

        metadata['identity'] = json.loads(metadata['identity'])
        
        identity = Identity.from_dict(metadata['identity'])

        if identity.is_bundle:
            r =  self.remote.datasets(identity.vid).post(metadata)
            raise_for_status(r)
        else:
            r =  self.remote.datasets(identity.as_dataset.vid).partitions(identity.vid).post(metadata)
            raise_for_status(r)

        return r

    def find(self, query):
        '''Find datasets, given a QueryCommand object'''
        from ambry.library.query import QueryCommand
        from ambry.identity import Identity, new_identity

        if isinstance(query, basestring):
            query = query.replace('/','|')
            response =  self.remote.datasets.find(query).get()
            raise_for_status(response)
            r = response.object


        elif isinstance(query, dict):
            # Dict form of  QueryCOmmand
            response =  self.remote.datasets.find.post(query)
            raise_for_status(response)
            r = response.object
            
        elif isinstance(query, QueryCommand):
            response =  self.remote.datasets.find.post(query.to_dict())
            raise_for_status(response)
            r = response.object
            
        else:
            raise ValueError("Unknown input type: {} ".format(type(query)))
        
        
        raise_for_status(response)

        return r
      
    
    def list(self):
        '''Return a list of all of the datasets in the library'''
        response =   self.remote.datasets.get()
        raise_for_status(response)
        return response.object
            
    def dataset(self, name_or_id):
        
        ref = self.get_ref(name_or_id)
        
        if not ref:
            return False
        
        id =  ref['dataset']['id']
        
        response =   self.remote.datasets(id).info().get()
        raise_for_status(response)
        return response.object
        
            
    def close(self):
        '''Close the server. Only used in testing. '''
        response =   self.remote.test.close.get()
        raise_for_status(response)
        return response.object

    
    def backup(self):
        '''Tell the server to backup its library to the remote'''
        response =   self.remote.backup.get()
        raise_for_status(response)
        return response.object
    
    

    
        