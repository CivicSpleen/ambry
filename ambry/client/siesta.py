#    Python Siesta
#
#    Copyright (c) 2008 Rafael Xavier de Souza
#
#    Modified by Eric Busboom
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""
Siesta is a REST client for python
"""

#__all__ = ["API", "Resource"]
__version__ = "0.5.1"
__author__ = "Sebastian Castillo <castillobuiles@gmail.com>"
__contributors__ = []

import sys
import re
import time
import urllib
import httplib
import simplejson as json

from urlparse import urlparse
from ambry.util import get_logger


import logging #@UnusedImport
import logging.handlers #@UnusedImport
logger = get_logger(__name__)
#logger.setLevel(logging.DEBUG) 
        
USER_AGENT = "Python-siesta/%s" % __version__

class ServerError(Exception):
    pass

class Response(object):
    object = None
    is_error = False
    status = None
    message = None
    headers = None
    content_type = None
    http_response = None
    exception = None
   
    def to_dict(self):
        return {
                'object' : self.object,
                'is_error' : self.is_error,
                'status' : self.status,
                'message' : self.message,
                'headers' : self.headers,
                'content_type' : self.content_type,
                'http_response' : self.http_response,
                'exception' : self.exception
                }
    
    def __init__(self, resp):
        
        self.http_response = resp
        
        if resp.status == 202:
            return self.handle_202(resp)

        self.content_type, encoding = self.get_mime_type(resp) #@UnusedVariable

        self.object = self.type_handlers.get(self.content_type, self.type_handlers.get('default'))(self,resp)
    
        self.status = int(resp.status)
        self.message = resp.reason
        self.headers = resp.getheaders()
        
 
        if isinstance(self.object, dict) and self.object.get('exception', False):
            self.exception = self.handle_exception()
        else:
            self.exception = None
            
        if self.status >= 400:
            self.is_error = True
            
        if self.status >= 500:
            self.exception = ServerError('Server error. See server log for details')    
       
    def get_header(self, key):
        for h in self.headers: 
            if h[0].lower() == key.lower():
                return h[1]        

       
    def handle_exception(self): 
        '''If self.object has an exception, re-construct the exception and 
        return it, to be raised later'''  
        
        import types
        
        field = self.object['exception']['class']

        pre_message = ''
        try:
            class_ = getattr(sys.modules['ambry.client.exceptions'], field)
        except AttributeError:
            pre_message = "(Class: {}.) ".format(field)
            class_=Exception
            
        if not isinstance(class_, (types.ClassType, types.TypeType)):
            pre_message = "(Class: {},) ".format(field)
            class_=Exception

        
        args = self.object['exception']['args']
        
        # Add the pre-message, if the real exception type is not known. 
        if isinstance(args, list) and len(args) > 0:
            args[0] = pre_message + str(args[0])

        # Add the trace
        try:
            if args:
                args[0] = args[0] + "\n---- Server Trace --- \n" + self.object['exception']['trace']
            else:
                args.append("\n---- Server Trace --- \n" + self.object['exception']['trace'])
        except:
            print "Failed to augment exception. {}, {}".format(args, self.object)
        return  class_(*args)       
    
    def read(self, count):
        return self.http_response.read(count)
            
    def get_mime_type(self, resp):
        content_type  = resp.getheader('content-type')
        m = None
        
        if content_type:
            m = re.match('^([^;]*)(?:; charset=(.*))?$',content_type)
            
        if m == None:
            mime, encoding = ('', '')
        else:
            mime, encoding = m.groups()
            
        return mime, encoding
            
    def handle_json_object(self, resp):
        body = resp.read()
        o = json.loads(body)
        resp.close()
        return o
    
    def handle_xml_object(self, resp):
        from xml.dom import minidom
        o = minidom.parse(resp)
        resp.close()
       
        return o
       
    def handle_html_object(self, resp):
        o = resp.read()
        resp.close()
        return o
    
    def handle_default_object(self, resp):
        pass

    type_handlers = {
        'application/json' : handle_json_object,
        'application/xml' : handle_xml_object, 
        'text/html' : handle_html_object,
        'default' : handle_default_object }


    def handle_202(self, resp):
        status_url = resp.getheader('content-location')
        if not status_url:
            raise Exception('Empty content-location from server')

        status_uri = urlparse(status_url).path
        status, st_resp  = Resource(uri=status_uri, remote=self.remote).get()
        retries = 0
        MAX_RETRIES = 3
        resp_status = st_resp.status

        while resp_status != 303 and retries < MAX_RETRIES:
            retries += 1
            status.get()
            time.sleep(5)
            
        if retries == MAX_RETRIES:
            raise Exception('Max retries limit reached without success')
        
        location = status.conn.getresponse().getheader('location')
        resource = Resource(uri=urlparse(location).path, remote=self.remote).get()
        return resource, None

class Resource(object):

    # TODO: some attrs could be on a inner meta class
    # so Resource can have a minimalist namespace  population
    # and minimize collitions with resource attributes
    def __init__(self, uri, api):
        #logging.info("init.uri: %s" % uri)
        self.remote = api
        self.uri = uri
        self.scheme, self.host, self.url, z1, z2 = httplib.urlsplit(self.remote.base_url + self.uri) #@UnusedVariable
        self.id = None
        self.conn = None
        self.headers = {'User-Agent': USER_AGENT}
        self.attrs = {}
        self._errors = {}
        
    def __getattr__(self, name):
        """
        Resource attributes (eg: user.name) have priority
        over inner rerouces (eg: users(id=123).applications)
        """
        #logging.info("getattr.name: %s" % name)
        # Reource attrs like: user.name
        if name in self.attrs:
            return self.attrs.get(name)
        #logging.info("self.url: %s" % self.url)
        # Inner resoruces for stuff like: GET /users/{id}/applications
        
        if name == 'root':
            key = self.uri
        else:
            key = self.uri + '/' + name
        self.remote.resources[key] = Resource(uri=key,
                                           api=self.remote)
        return self.remote.resources[key]

    def __call__(self, id_=None):
        #logging.info("call.id_: %s" % id_)
        #logging.info("call.self.url: %s" % self.url)
        if id_ == None:
            return self
        self.id = str(id_)
        key = self.uri + '/' + self.id
        self.remote.resources[key] = Resource(uri=key,
                                           api=self.remote)
        return self.remote.resources[key]

    # Set the "Accept" request header.
    # +info about request headers:
    # http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html
    def set_request_type(self, mime):
        if mime.lower() == 'json':
            mime = 'application/json'
        elif mime.lower() == 'xml':
            mime = 'application/xml'
        self.headers['Accept'] = mime

    # GET /resource
    # GET /resource/id?arg1=value1&...
    def get(self, **kwargs): 
        return self.do_method('GET', None, None, kwargs)  
      
    # POST /resource
    def post(self, data, **kwargs):
        return self.do_method('POST', None, data, kwargs)  

    # PUT /resource/id
    def put(self, data, **kwargs):
        return self.do_method('PUT', None, data, kwargs)  
 
    # DELETE /resource/id
    def delete(self, id=None, **kwargs):
        return self.do_method('DELETE', id, None, kwargs)  

    def do_method(self, method, id_, data, kwargs):
        if self.id == None:
            url = self.url
        else:
            url = self.url + '/' + str(self.id)
            
        if len(kwargs) > 0:
            url = "%s?%s" % (url, urllib.urlencode(kwargs))
            
        meta = dict([(k, kwargs.pop(k)) for k in kwargs.keys() if k.startswith("__")])
          
        logger.debug("Requesting {} {}".format(method, url))
        self._request(method, url, data, {}, meta)
          
        return self._getresponse()
        

    def _request(self, method, url, body={}, headers={}, meta={}):
        import socket
        
        if self.remote.auth:
            headers.update(self.remote.auth.make_headers())
        
        if self.conn != None:
            self.conn.close()

        if not 'User-Agent' in headers:
            headers['User-Agent'] = self.headers['User-Agent']
        if not 'Accept' in headers and 'Accept' in self.headers:
            headers['Accept'] = self.headers['Accept']

        if self.scheme == "http":
            self.conn = httplib.HTTPConnection(self.host)
        elif self.scheme == "https":
            self.conn = httplib.HTTPSConnection(self.host)
        else:
            raise IOError("unsupported protocol: %s" % self.scheme)

        if isinstance(body, basestring):
            headers = {"Content-Type": "text/plain"}
            pass
        elif  hasattr(body, 'read'):
            # File like object, httplib can handle it, so just pass it through. 
            headers = {"Content-Type": "application/octet-stream"}
            pass
        else:
            headers = {"Content-Type": "application/json"}
            body = json.dumps(body)

                
        try:   
            self.conn.request(method, url, body, headers)
        except IOError as e: 
            raise IOError("Request to {} failed: {} ".format(url, e.message))
           

    def _getresponse(self):
        resp = self.conn.getresponse()
      
        ro =  Response(resp)
     
        if ro.exception is not None:
            raise ro.exception
    
        return ro

class API(object):
    def __init__(self, base_url, auth=None):
        self.base_url = base_url + '/' if not base_url.endswith('/') else base_url
        self.api_path = urlparse(base_url).path
        self.resources = {}
        self.request_type = None
        self.auth = auth

    def set_request_type(self, mime):
        self.request_type = mime
        # set_request_type for every instantiated resources:
        for resource in self.resources:
            self.resources[resource].set_request_type(mime)

    def __getattr__(self, name):

        key = name
        if not key in self.resources:
            self.resources[key] = Resource(uri=key,api=self)
        return self.resources[key]
