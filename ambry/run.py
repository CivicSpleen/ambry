"""Runtime configuration logic for running a bundle build. 

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

import os.path
from ambry.util import AttrDict
from ambry.util import lru_cache

@lru_cache()
def get_runconfig(path=None,  is_server=False):
    return RunConfig(path, is_server)

class RunConfig(object):
    '''Runtime configuration object 
    
    The RunConfig object will search for a ambry.yaml file in multiple locations,
    including::
    
      /etc/ambry.yaml
      ~user/.ambry.yaml
      ./ambry.yaml
      A named path ( --config option )
      
    It will start from the first directory, and for each one, try to load the file
    and copy the values into an accumulator, with later values overwritting
    earlier ones. 
    '''

    ROOT_CONFIG = '/etc/ambry.yaml'
    USER_CONFIG = os.path.expanduser('~/.ambry.yaml')
    USER_ACCOUNTS = os.path.expanduser('~/.ambry-accounts.yaml')
    DIR_CONFIG = os.path.join(os.getcwd(),'ambry.yaml')

    config = None
    files = None

    def __init__(self, path=None, is_server = False):
        '''Create a new RunConfig object
        
        Arguments
        path -- If present, a yaml file to load last, overwriting earlier values.
          If it is an array, load only the files in the array. 

        '''

        config = AttrDict()
        config['loaded'] = []

    
        if isinstance(path, (list, tuple, set)):
            files = path
        else:
            files = [
                          RunConfig.ROOT_CONFIG,
                          RunConfig.USER_CONFIG,
                          RunConfig.USER_ACCOUNTS,
                          RunConfig.DIR_CONFIG, 
                          path]

        loaded = False

        for f in files:
            
            if f is not None and os.path.exists(f):
                try:
                    loaded = True

                    config.loaded.append(f)
                    config.update_yaml(f)
                except TypeError:
                    pass # Empty files will produce a type error

        if not loaded:
            raise Exception("Failed to load any config from: {}".format(files))

        object.__setattr__(self, 'config', config)
        object.__setattr__(self, 'files', files)

    def __getattr__(self, group):
        '''Fetch a configuration group and return the contents as an
        attribute-accessible dict'''

        return self.config.get(group,{})


    def __setattr__(self, group, v):
        '''Fetch a configuration group and return the contents as an
        attribute-accessible dict'''

        self.config[group] = v

    def get(self,k, default=None):

        if not default:
            default = None

        return self.config.get(k,default)

    def group(self, name):
        '''return a dict for a group of configuration items.'''
        
        return self.config.get(name,{})

    def group_item(self, group, name):
        import copy
        from dbexceptions import ConfigurationError
        
        g = self.group(group)
        
        if not name in g:
            raise ConfigurationError(("Could not find name '{}' in group '{}'. \n"+
                                      "Config has: {}\nLoaded: {}").format(name, group, g.keys(), self.loaded))
        
        return  copy.deepcopy(g[name])
        

    def _yield_string(self, e):
        '''Recursively descend a data structure to find string values.
         This will locate values that should be expanded by reference. '''
        from util import walk_dict

        for path, subdicts, values in walk_dict(e):
            for k,v in values:
                
                if v is None:
                    import pprint
                    pprint.pprint(e.to_dict())
                    raise Exception('Got None value: {} {} {} {} '.format(path, subdicts, k, v))
                
                path_parts = path.split('/')
                path_parts.pop()
                path_parts.pop(0)
                path_parts.append(k)
                def setter(nv):
                    sd = e
                    for pp in path_parts:
                        if not isinstance(sd[pp], dict ):
                            break
                        sd = sd[pp]
                        
                    # Save the Original value as a name
                        
                    sd[pp] = nv
                    
                    if isinstance(sd[pp], dict):
                        sd[pp]['_name'] = v
            
                yield k,v,setter
        
    
    def _sub_strings(self, e, subs):
        '''Substitute keys in the dict e with functions defined in subs'''

        iters = 0
        while (iters < 100):
            sub_count = 0
           
            for k,v,setter in self._yield_string(e):
                if k in subs:
                    setter(subs[k](k,v))
                    sub_count += 1

            if sub_count == 0:
                break

            iters += 1   
            
            
            
        return e         

    def dump(self, stream=None):
        
        to_string = False
        if stream is None:
            import StringIO
            stream = StringIO.StringIO()
            to_string = True
            
        self.config.dump(stream)
        
        if to_string:
            stream.seek(0)
            return stream.read()
        else:
            return stream
        

    def filesystem(self,name):
        e =  self.group_item('filesystem', name) 

        # If the value is a string, rather than a dict, it is for a
        # FsCache. Re-write it to be the expected type.

        if isinstance(e, basestring):
            e = dict(dir=e)


        fs = self.group('filesystem') 
        root_dir = fs['root'] if 'root' in fs  else  '/tmp/norootdir'



        e =  self._sub_strings(e, {
                                     'upstream': lambda k,v: self.filesystem(v),
                                     'account': lambda k,v: self.account(v),
                                     'dir' : lambda k,v: v.format(root=root_dir)
                                     }  )

        return e
    
    def account(self,name):
        e = self.group_item('accounts', name) 

        e =  self._sub_strings(e, {'store': lambda k,v: self.filesystem(v)}  )


        e['_name'] = name
        
        return e


    def datarepo(self,name):
        e =  self.group_item('datarepo', name) 

        return self._sub_strings(e, {
                                     'filesystem': lambda k,v: self.filesystem(v)
                                     }  )
   
    def library(self,name):
        e =  self.group_item('library', name)

        fs = self.group('filesystem')
        root_dir = fs['root'] if 'root' in fs  else  '/tmp/norootdir'

        e =  self._sub_strings(e, {
                                     'filesystem': lambda k,v: self.filesystem(v),
                                     'database': lambda k,v: self.database(v),
                                     'upstream': lambda k,v: self.filesystem(v),
                                     'account': lambda k, v: self.account(v),
                                     'cdn': lambda k,v: self.account(v),
                                     'source': lambda k, v: v.format(root=root_dir)
                                     }  )

        e['_name'] = name

        return e
    

    
    def warehouse(self,name):
        e =  self.group_item('warehouse', name) 

        return self._sub_strings(e, {
                                     'database': lambda k,v: self.database(v),
                                     'account': lambda k,v: self.account(v),
                                     'library': lambda k,v: self.database(v),
                                     }  )
    def database(self,name):
        
        fs = self.group('filesystem') 
        root_dir = fs['root'] if 'root' in fs  else  '/tmp/norootdir'
        
        e = self.group_item('database', name) 

        e =  self._sub_strings(e, {'dbname' : lambda k,v: v.format(root=root_dir),
                                   'account': lambda k,v: self.account(v),}  )

        # Copy account credentials into the database record, so there is consistent access
        # pattern
        if 'account' in e:
            account = e['account']
            if 'password' in account:
                e['user'] = account['user']
                e['password'] = account['password']
                
        e = e.to_dict()

        return e


    def python_dir(self):

        fs = self.group('filesystem') 
        
        if not 'python' in fs:
            return None
        
        root_dir = fs['root'] if 'root' in fs  else  '/tmp/norootdir'

        python_dir = fs['python'].format(root=root_dir)

      
        return python_dir


    def filesystem_path(self, name):

        fs = self.group('filesystem')

        if not name in fs:
            return None

        root_dir = fs['root'] if 'root' in fs  else  '/tmp/norootdir'

        path = fs[name].format(root=root_dir)

        return path


    @property
    def dict(self):
        return self.config.to_dict()


def mp_run(mp_run_args):
    ''' Run a bundle in a multi-processor child process. '''
    import traceback, sys
    
    bundle_dir, run_args, method_name, args = mp_run_args

    try:
        
        bundle_file = sys.argv[1]
        
        if not os.path.exists(os.path.join(os.getcwd(), 'bundle.yaml')):
            print >> sys.stderr, "ERROR: Current directory '{}' does not have a bundle.yaml file, so it isn't a bundle file. Did you mean to run 'cli'?".format(os.getcwd())
            sys.exit(1)
    
        # Import the bundle file from the 
        rp = os.path.realpath(os.path.join(bundle_dir, 'bundle.py'))
        mod = import_file(rp)
     
        dir_ = os.path.dirname(rp)
        b = mod.Bundle(dir_)
        b.run_args = AttrDict(run_args)

        method = getattr(b, method_name)

        b.log("MP Run: pid={} {}{} ".format(os.getpid(),method.__name__, args))

        try:
            method(*args)
        except:
            b.close()
            raise
        
    except:
        tb = traceback.format_exc()
        print '==========vvv MR Run Exception: {}==========='.format(args)
        print tb
        print '==========^^^ MR Run Exception: {}==========='.format(args)
        raise


def import_file(filename):
    ''' '''
    import imp
    (path, name) = os.path.split(filename)
    (name, _) = os.path.splitext(name)
    (_, modname) = os.path.split(path)

    modname = modname.replace('.','_') # To avoid 'Parent module not found' warnings

    (file_, filename, data) = imp.find_module(name, [path])

    return imp.load_module(modname, file_, filename, data)

if __name__ == '__main__':
    '''When bambry is run, this routine will load the bundle module from a file
    wire it into the namespace and run it with the arguments passed into bambry. '''
    import sys
    args = list(sys.argv)

    bundle_file = sys.argv[1]
    
    if not os.path.exists(os.path.join(os.getcwd(), 'bundle.yaml')):
        print >> sys.stderr, "ERROR: Current directory '{}' does not have a bundle.yaml file, so it isn't a bundle file. Did you mean to run 'cli'?".format(os.getcwd())
        sys.exit(1)

    # Import the bundle file from the 
    rp = os.path.realpath(os.path.join(os.getcwd(), bundle_file))
    mod = import_file(rp)
 
    dir_ = os.path.dirname(rp)
    b = mod.Bundle(dir_)

    b.run(args[2:])     

    
   
    
    