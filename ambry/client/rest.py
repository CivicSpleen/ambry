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


class RestApi(object):

    _url = None

    def __init__(self, url):
        self._url = url

    def url(self,u,*args, **kwargs):

        if u.startswith("http"):
            return u

        if len(u) > 0 and u[0] == '/':
            u = u[1:]


        return self._url+'/'+u.format(*args, **kwargs)

    def get(self, url, params={}):

        r = requests.get(self.url(url), params=params)
        self.handle_status(r)

        return self.handle_return(r)

    def head(self, url, params={}):

        r = requests.head(self.url(url), params=params)
        self.handle_status(r)

        return self.handle_return(r)


    def get_stream(self, url, params={}):

        r = requests.get(self.url(url), verify=False, stream=True)

        self.handle_status(r)

        if r.headers.get('content-encoding', '') == 'gzip':
            from ..util import FileLikeFromIter
            # In  the requests library, iter_content will auto-decompress
            response = FileLikeFromIter(r.iter_content(chunk_size=128 * 1024))
        else:
            response = r.raw

        response.meta = {}

        for k, v in r.headers.items():
            if k.startswith('x-amz'):
                k = k.replace('x-amz','')

            response.meta[k] = v

        return response

    def put(self, url, params={}, data=None):

        if not isinstance(data,(list,dict)):
            data = [data]

        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

        r = requests.put(self.url(url), params=params, data=json.dumps(data), headers=headers)

        self.handle_status(r)

        return self.handle_return(r)

    def post(self, url, params={}, data=None):

        if not isinstance(data,(list,dict)):
            data = [data]

        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

        r = requests.post(self.url(url), params=params, data=json.dumps(data), headers=headers)

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
        tr = object['exception']['trace']
        if isinstance(tr, list):
            tr = '\n'.join(tr)

        try:
            if args:
                args[0] = str(args[0]) + "\n---- Server Trace --- \n" + tr
            else:
                args.append("\n---- Server Trace --- \n" + tr)
        except Exception as e:
            print "Failed to augment exception. {}, {}".format(args, object)
            print 'AAA', e, e.message

        return  class_(*args)


class RemoteLibrary(RestApi,object):

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
        tr = object['exception']['trace']
        if isinstance(tr, list):
            tr = '\n'.join(tr)

        try:
            if args:
                args[0] = str(args[0]) + "\n---- Server Trace --- \n" + tr
            else:
                args.append("\n---- Server Trace --- \n" + tr)
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

        url = self.url("/info/{}", ref)

        return self.get(url)

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

