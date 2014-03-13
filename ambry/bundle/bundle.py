"""The Bundle object is the root object for a bundle, which includes acessors 
for partitions, schema, and the filesystem

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from ..filesystem import  BundleFilesystem
from ..schema import Schema
from ..partitions import Partitions

import os.path
from ..dbexceptions import  ConfigurationError, ProcessError

from config import *


class Bundle(object):
    '''Represents a bundle, including all configuration 
    and top level operations. '''
 
    logger = None
 
    def __init__(self, logger=None):
        '''
        '''

        self._schema = None
        self._partitions = None
        self._library = None
        self._identity = None
        self._repository = None
        self._dataset_id = None # Needed in LibraryDbBundle to  disambiguate multiple datasets

        
        # This bit of wackiness allows the var(self.run_args) code
        # to work when there have been no artgs parsed. 
        class null_args(object):
            none = None
            multi = False
            test = False

        self.run_args = vars(null_args())

        self._logger = logger

    @property
    def logger(self):

        if not self._logger:
            from ..util import get_logger
            import logging

            try:
                ident = self.identity
                template = ident.sname+" %(message)s"
            except:
                template = "%(message)s"

            self._logger = get_logger(__name__, template=template)


            self.logger.setLevel(logging.INFO)

        return self._logger

        
    @property
    def schema(self):
        if self._schema is None:
            self._schema = Schema(self)


        return self._schema
    
    @property
    def partitions(self):     
        if self._partitions is None:
            self._partitions = Partitions(self)  
            
        return self._partitions

    @property
    def repository(self):
        '''Return a repository object '''
        from ..repository import Repository #@UnresolvedImport

        if not self._repository:
            repo_name = 'default'
            self._repository =  Repository(self, repo_name)
            
        return self._repository
    

    def get_dataset(self, session):
        '''Return the dataset
        '''
        from sqlalchemy.orm.exc import NoResultFound
        from sqlalchemy.exc import OperationalError
        from ..dbexceptions import NotFoundError
        
        from ambry.orm import Dataset

        try:
            if self._dataset_id:
                try:
                    return (session.query(Dataset)
                            .filter(Dataset.location == Dataset.LOCATION.LIBRARY)
                            .filter(Dataset.vid == self._dataset_id).one())
                except NoResultFound:
                    from ..dbexceptions import NotFoundError

                    raise NotFoundError("Failed to find dataset for id {} in {} "
                                        .format(self._dataset_id, self.database.dsn))
            else:

                return (session.query(Dataset).one())
        except OperationalError:
            raise NotFoundError("No dataset record found. Probably not a bundle")
        except Exception as e:
            from ..util import get_logger
            # self.logger can get caught in a recursion loop
            logger = get_logger(__name__)
            logger.error("Failed to get dataset: {}; {}".format(e.message, self.database.dsn))
            raise

    @property
    def dataset(self):
        '''Return the dataset'''
        return self.get_dataset(self.database.session)

       
    def _dep_cb(self, library, key, name, resolved_bundle):
        '''A callback that is called when the library resolves a dependency.
        It stores the resolved dependency into the bundle database'''

        if resolved_bundle.partition:
            ident = resolved_bundle.partition.identity
        else:
            ident = resolved_bundle.identity
    
        if not self.database.is_empty():
            with self.session:
                self.db_config.set_value('rdep', key, ident.dict)

    @property
    def library(self):
        '''Return the library set for the bundle, or 
        local library from get_library() if one was not set. '''

        from ..library import new_library

        if self._library:
            l = self._library
        else:
            l = new_library(self.config.config.library('default'))

        l.logger =\
            self.logger
        l.database.logger = self.logger
        l.bundle = self
        l.dep_cb = self._dep_cb
        
        return l

    @library.setter
    def library(self, value):
        self._library = value

    @property
    def path(self):
        """Return the base path for the bundle, usually the path to the
        bundle database, but withouth the database extension."""
        raise NotImplementedError("Abstract")

    def sub_dir(self, *args):
        """Return a subdirectory relative to the bundle's database root path
        which based on the path of the database. For paths relative to the
        directory of a BuildBundle, use the Filesystem object. """
        return  os.path.join(self.path,*args)
    
    def query(self,*args, **kwargs):
        """Convience function for self.database.connection.execute()"""
        return self.database.query(*args, **kwargs)
    
    def log(self, message, **kwargs):
        '''Log the messsage'''
        self.logger.info(message)

    def error(self, message, **kwargs):
        '''Log an error messsage'''
        self.logger.error(message)
     
    def warn(self, message, **kwargs):
        '''Log an error messsage'''
        self.logger.warn(message)
        
    def fatal(self, message, **kwargs):
        '''Log a fata messsage and exit'''
        import sys 
        self.logger.fatal(message)
        sys.stderr.flush()
        if self.exit_on_fatal:
            sys.exit(1)
        else:
            from ..dbexceptions import FatalError
            raise FatalError(message)
    
class DbBundle(Bundle):

    def __init__(self, database_file, logger=None):
        '''Initialize a db and all of its sub-components. 
        
        If it does not exist, creates the db database and initializes the
        Dataset record and Config records from the db.yaml file. Through the
        config object, will trigger a re-load of the db.yaml file if it
        has changed. 
        
        Order of operations is:
            Create db.db if it does not exist
        '''
        from ..database.sqlite import SqliteBundleDatabase #@UnresolvedImport

        super(DbBundle, self).__init__(logger=logger)
       
        self.database_file = database_file

        self.database = SqliteBundleDatabase(self, database_file)

        self.db_config = self.config = BundleDbConfig(self, self.database)
        
        self.partition = None # Set in Library.get() and Library.find() when the user requests a partition. 

    @property
    def path(self):
        base, _ = os.path.splitext(self.database_file)
        return base
        
    def sub_path(self, *args):
        '''For constructing paths to partitions'''

        return os.path.join(self.path, *args)

    def table_data(self, query):
        '''Return a petl container for a data table'''
        import petl 
        query = query.strip().lower()
        
        if 'select' not in query:
            query = "select * from {} ".format(query)
 
        return petl.fromsqlite3(self.database.path, query) #@UndefinedVariable

    @property
    def identity(self):
        '''Return an identity object. '''
        from ..identity import Identity

        if not self._identity:
           self._identity = self.get_dataset(self.database.session).identity


        return self._identity

class LibraryDbBundle(Bundle):
    '''A database bundle that is built in place from the data in a library '''

    def __init__(self, database, dataset_id, logger=None):
        '''Initialize a db and all of its sub-components. 

        '''

        super(LibraryDbBundle, self).__init__(logger=logger)
   
        self._dataset_id = dataset_id
        self.database = database

        self.db_config = self.config = BundleDbConfig(self, self.database)
        
        self.partition = None # Set in Library.get() and Library.find() when the user requests a partition. s
        
    @property
    def path(self):
        raise NotImplemented()
        
    def sub_path(self, *args):
        '''For constructing paths to partitions'''
        raise NotImplemented() 
        
    @property
    def identity(self):
        '''Return an identity object. '''
        from ..identity import Identity

        if not self._identity:
           self._identity = self.get_dataset(self.database.session).identity


        return self._identity

class BuildBundle(Bundle):
    '''A bundle class for building bundle files. Uses the bundle.yaml file for
    identity configuration '''

    META_COMPLETE_MARKER = '.meta_complete'
    SCHEMA_FILE = 'schema.csv'
    SCHEMA_REVISED_FILE = 'schema-revised.csv'
    SCHEMA_OLD_FILE = 'schema-old.csv'

    def __init__(self, bundle_dir=None):
        '''
        '''

        super(BuildBundle, self).__init__()
        

        if bundle_dir is None:
            import inspect
            bundle_dir = os.path.abspath(os.path.dirname(inspect.getfile(self.__class__)))
          
        if bundle_dir is None or not os.path.isdir(bundle_dir):
            from ambry.dbexceptions import BundleError
            raise BundleError("BuildBundle must be constructed on a cache. "+
                              str(bundle_dir) + " is not a directory")
  
        self.bundle_dir = bundle_dir
    
        self._database  = None
   
        # For build bundles, always use the FileConfig though self.config
        # to get configuration. 
        self.config = BundleFileConfig(self.bundle_dir)

        self.filesystem = BundleFilesystem(self, self.bundle_dir)
        

        import base64
        self.logid = base64.urlsafe_b64encode(os.urandom(6)) 
        self.ptick_count = 0;

        # Library for the bundle
        lib_dir = self.filesystem.path('lib')
        if os.path.exists(lib_dir):
            import sys
            sys.path.append(lib_dir)

        self._build_time = None
        self._update_time = None
       
        self.exit_on_fatal = True
       
    @property
    def build_dir(self):
        
        try:
            cache = self.filesystem.get_cache_by_name('build')
            return cache.cache_dir
        except ConfigurationError:
            return  self.filesystem.path(self.filesystem.BUILD_DIR)
        
       
    @property
    def path(self):
        return os.path.join(self.build_dir, self.identity.path) 

    @property
    def db_path(self):
        return self.path+'.db'

    def sub_path(self, *args):
        '''For constructing paths to partitions'''

        return os.path.join(self.build_dir, self.identity.path, *args)

    @property
    def database(self):
        from ..database.sqlite import BuildBundleDb #@UnresolvedImport

        if self._database is None:
            self._database  = BuildBundleDb(self, self.db_path)

        return self._database

    @property
    def session(self):
        return self.database.lock

    @property
    def has_session(self):
        return self.database.has_session

    @property
    def db_config(self):
        return BundleDbConfig(self, self.database)

    @property
    def identity(self):
        '''Return an identity object. '''
        from ..identity import Identity, Name, ObjectNumber

        if not self._identity:
            try:
                names = self.config.names.items()
                idents = self.config.identity.items()
            except AttributeError:
                raise AttributeError("Failed to get required sections of config. "+
                                    "\nconfig_source = {}\n".format(self.config.source_ref))
            self._identity =  Identity.from_dict(dict(names+idents))


        return self._identity

    def update_configuration(self):
        from ..dbexceptions import DatabaseError
        # Re-writes the undle.yaml file, with updates to the identity and partitions
        # sections. 
        from ..dbexceptions import  DatabaseMissingError

        if self.database.exists():
            partitions = [ { k:v for k,v in p.identity.dict.items() if k in ['id', 'name','vname','cache_key'] }
                           for p in self.partitions]

        else:
            partitions = []

        self.config.rewrite(
                         identity=self.identity.ident_dict,
                         names=self.identity.names_dict,
                         partitions=partitions
                         )
        
        # Reload some of the values from bundle.yaml into the database configuration

        if self.database.exists():

            if self.config.build.get('dependencies'):
                dbc = self.db_config
                for k,v in self.config.build.get('dependencies').items():
                    dbc.set_value('odep', k, v)

            self.database.rewrite_dataset()
                
        
    @classmethod
    def rm_rf(cls, d):
        
        if not os.path.exists(d):
            return

        for path in (os.path.join(d,f) for f in os.listdir(d)):
            if os.path.isdir(path):
                cls.rm_rf(path)
            else:
                os.unlink(path)
        os.rmdir(d)
       
    @property 
    def sources(self):
        """Return a dictionary of sources from the build.sources configuration key"""

        if not self.config.group('build'): 
            raise ConfigurationError("Configuration does not have 'build' group")
        if not self.config.group('build').get('sources',None):
            raise ConfigurationError("Configuration does not have 'build.sources' group")
            
        return self.config.build.sources
        
    def source(self,name):
        """Return a source URL with the given name, from the build.sources configuration
        value"""
        
        s = self.config.build.sources
        return s.get(name, None)
        
    
        

    def clean(self, clean_meta=False):
        '''Remove all files generated by the build process'''

        # Remove partitions
        self.rm_rf(self.sub_path())
        # Remove the database
        
        if self.database.exists():
            self.database.delete()
        
        
        if clean_meta:
            mf = self.filesystem.meta_path(self.META_COMPLETE_MARKER)
            if os.path.exists(mf):
                os.remove(mf)
        
        ed = self.filesystem.path('extracts')
        if os.path.exists(ed):
            self.rm_rf(ed)
        
        # Should check for a shared download file -- specified
        # as part of the library; Don't delete that. 
        #if not self.cache_downloads :
        #    self.rm_rf(self.filesystem.downloads_path())

        self.library.source.set_bundle_state(self.identity, 'cleaned')

    def progress(self,message):
        '''print message to terminal, in place'''
        print 'PRG: ',message

    def ptick(self,message):
        '''Writes a tick to the stdout, without a space or newline'''
        import sys
        sys.stdout.write(message)
        sys.stdout.flush()
        
        self.ptick_count += len(message)
       
        if self.ptick_count > 72:
            sys.stdout.write("\n")
            self.ptick_count = 0

    def init_log_rate(self, N=None, message='', print_rate=None):
        from ..util import init_log_rate as ilr
        
        return ilr(self.log, N=N, message=message, print_rate = print_rate)



    ### Prepare is run before building, part of the devel process.  

    def pre_meta(self):
        '''Skips the meta stage if the :class:.`META_COMPLETE_MARKER` file already exists'''

        mf = self.filesystem.meta_path(self.META_COMPLETE_MARKER)


        if os.path.exists(mf) and not self.run_args.get('clean',None):
            self.log("Meta information already generated")
            #raise ProcessError("Bundle has already been prepared")
            return False
        

        return True

    def meta(self):
        return True
    
    def post_meta(self):
        '''Create the :class:.`META_COMPLETE_MARKER` meta marker so we don't run the meta process again'''
        import datetime
        mf = self.filesystem.meta_path(self.META_COMPLETE_MARKER)
        with open(mf,'w+') as f:
            f.write(str(datetime.datetime.now()))
    
        return True

    ### Prepare is run before building, part of the devel process.  



    def pre_prepare(self):

        self.log('---- Pre-Prepare ----')
        
        if self.config.build.get('requirements',False):
            from ..util.packages import install
            import sys
            import imp
            
            python_dir = self.config.config.python_dir()
            
            if not python_dir:
                raise ConfigurationError("Can't install python requirements without a configuration item for filesystems.python")
            
            if not os.path.exists(python_dir):
                os.makedirs(python_dir)
            
                
            sys.path.append(python_dir)
            
            self.log("Installing required packages in {}".format(python_dir))
            
            for k,v in self.config.build.requirements.items():
                
                try:
                    imp.find_module(k)
                    self.log("Required package already installed: {}->{}".format(k,v))
                except ImportError:
                    self.log("Installing required package: {}->{}".format(k,v))
                    install(python_dir,k,v)

        
        if self.is_prepared:
            self.log("Bundle has already been prepared")
            #raise ProcessError("Bundle has already been prepared")
     
            return False
        
        try:
            b = self.library.resolve(self.identity.id_)
            
            if b and b.on.revision >= self.identity.on.revision:
                self.fatal(("Can't build this version. Library {} has version {} of {}"
                            " which is less than or equal this version {}")
                           .format(self.library.database.dsn, b.on.revision, b.fqname, self.identity.on.revision))
                return False
            
        except Exception as e:
            raise
            self.error("Error in consulting library: {}\nException: {}".format(self.library.info,e.message))

        return True

    def _prepare_load_schema(self):

        sf = self.filesystem.path(self.config.build.get('schema_file', 'meta/' + self.SCHEMA_FILE))
        if os.path.exists(sf):
            with open(sf, 'rbU') as f:
                self.log("Loading schema from file: {}".format(sf))
                self.schema.clean()
                with self.session:
                    warnings, errors = self.schema.schema_from_file(f)

                for title, s, f in (("Errors", errors, self.error), ("Warnings", warnings, self.warn)):
                    if s:
                        self.log("----- Schema {} ".format(title))
                        for table_name, column_name, message in s:
                            f("{:20s} {}".format(
                                "{}.{}".format(table_name if table_name else '', column_name if column_name else ''),
                                message))

                if errors:
                    self.fatal("Schema load filed. Exiting")
        else:
            self.log("No schema file ('{}') not loading schema".format(sf))


    def prepare(self):
        from ..dbexceptions import NotFoundError
        
        # with self.session: # This will create the database if it doesn't exist, but it will be empty
        if not self.database.exists():
            self.log("Creating bundle database")
            self.database.create()
        else:
            self.log("Bundle database already exists")

        try:
            self.library.check_dependencies()
        except NotFoundError as e:
            self.fatal(e.message)

        if self.run_args and self.run_args.get('rebuild',False):
            with self.session:
                self.rebuild_schema()
        else:
            self._prepare_load_schema()

        return True
    
    def rebuild_schema(self):
        sf  = self.filesystem.path(self.config.build.get('schema_file', 'meta/schema.csv'))
        with open(sf, 'rbU') as f:

            partitions = [p.identity for p in self.partitions.all]
            self.schema.clean()
            self.schema.schema_from_file(f)  
            
            for p in partitions:
                self.partitions.new_db_partition(p)

    
    def _revise_schema(self):
        '''Write the schema from the database back to a file. If the schema template exists, overwrite the
        main schema file. If it does not exist, use the revised file

        
        '''

        self.update_configuration()

        sf_out = self.filesystem.path('meta',self.SCHEMA_REVISED_FILE)

        # Need to expire the unmanaged cache, or the regeneration of the schema may 
        # use the cached schema object rather than the ones we just updated, if the schem objects
        # have alread been loaded. 
        self.database.unmanaged_session.expire_all()

        with open(sf_out, 'w') as f:
            self.schema.as_csv(f)    
                    
    def post_prepare(self):
        '''Set a marker in the database that it is already prepared. '''
        from datetime import datetime

        with self.session:
            self.db_config.set_value('process','prepared',datetime.now().isoformat())

            self._revise_schema()

        self.library.source.set_bundle_state(self.identity, 'prepared')

        return True

    @property
    def is_prepared(self):
        return ( self.database.exists() 
                 and not self.run_args.get('rebuild',False)
                 and  self.db_config.get_value('process','prepared', False))
   
    ### Build the final package

    def pre_build(self):
        from time import time
        import sys
        
        if not self.database.exists():
            raise ProcessError("Database does not exist yet. Was the 'prepare' step run?")

        if self.is_built and not self.run_args.get('force', False):
            self.log("Bundle is already build. Skipping  ( Use --clean  or --force to force build ) ")
            return False

        with self.session:
            if not self.db_config.get_value('process','prepared', False):
                raise ProcessError("Build called before prepare completed")
            
            self._build_time = time()
        
        python_dir = self.config.config.python_dir()
        
        if  python_dir and python_dir not in sys.path:
            sys.path.append(python_dir)
        

        return True
        
    def build(self):
        return False
    
    
    def post_build(self):
        '''After the build, update the configuration with the time required for the build, 
        then save the schema back to the tables, if it was revised during the build.  '''
        from datetime import datetime
        from time import time
        import shutil
        
          
        with self.session:
            self.db_config.set_value('process', 'built', datetime.now().isoformat())
            self.db_config.set_value('process', 'buildtime',time()-self._build_time)
            self.update_configuration()

            self._revise_schema()
        
        
        # Some original import files don't have a schema, particularly 
        # imported Shapefiles
        if os.path.exists(self.filesystem.path('meta',self.SCHEMA_FILE)):
            shutil.copy(
                        self.filesystem.path('meta',self.SCHEMA_FILE),
                        self.filesystem.path('meta',self.SCHEMA_OLD_FILE)
                        )
    
            shutil.copy(
                        self.filesystem.path('meta',self.SCHEMA_REVISED_FILE),
                        self.filesystem.path('meta',self.SCHEMA_FILE)
                        )
        

        self.post_build_write_stats()

        self.library.source.set_bundle_state(self.identity, 'built')

        return True
    
    def post_build_write_stats(self):
        from sqlalchemy.exc import OperationalError
        
        # Create stat entries for all of the partitions. 
        for p in self.partitions:
            try:
                from ..partition.sqlite import SqlitePartition
                from ..partition.geo import GeoPartition
                self.log("Writting stats for: {}".format(p.identity.name))
                if isinstance(p,(SqlitePartition, GeoPartition)):
                    self.log("Writting stats for: {}".format(p.identity.name))
                    p.write_stats()
                else:
                    self.log("Skipping stats for non db partition: {}".format(p.identity.name))

            except NotImplementedError:
                self.log("Can't write stats (unimplemented) for partition: {}".format(p.identity.name))
            except ConfigurationError as e:
                self.error(e.message)
            except OperationalError as e:
                self.error("Failed to write stats for partition {}: {}".format(p.identity.name, e.message))
                raise
                    
    
    @property
    def is_built(self):
        '''Return True is the bundle has been built'''
        
        if not self.database.exists():
            return False

        v = self.db_config.get_value('process','built', False)
        
        return bool(v)
        
        
    ### Update is like build, but calls into an earlier version of the package. 

    def pre_update(self):
        from time import time

        if not self.database.exists():
            raise ProcessError("Database does not exist yet. Was the 'prepare' step run?")
        
        if not self.db_config.get_value('process','prepared'):
            raise ProcessError("Update called before prepare completed")
        
        self._update_time = time()
        
        return True
        
    def update(self):
        return False
    
    def post_update(self):
        from datetime import datetime
        from time import time
        with self.session:
            self.db_config.set_value('process', 'updated', datetime.now().isoformat())
            self.db_config.set_value('process', 'updatetime',time()-self._update_time)
            self.update_configuration()
        return True
        
    ### Submit the package to the library
 
    def pre_install(self):
        
        with self.session:
            self.update_configuration()
        
        return True
    
    def install(self, library_name=None, delete=False,  force=False):  
        '''Install the bundle and all partitions in the default library'''
     
        import ambry.library

        force = self.run_args.get('force', force)

        with self.session:

            #library_name = self.run_args.get('library', 'default') if library_name is None else 'default'
            #library_name = library_name if library_name else 'default'
            #library = ambry.library.new_library(self.config.config.library(library_name), reset=True)
            library = self.library
         
            self.log("{} Install to  library {}".format(self.identity.name, library.database.dsn))
            dest = library.put_bundle(self, force=force)
            self.log("{} Installed".format(dest[1]))
            
            skips = self.config.group('build').get('skipinstall',[])
            
            for partition in self.partitions:
                
                if not os.path.exists(partition.database.path):
                    self.log("{} File does not exist, skipping".format(partition.database.path))
                    continue
                
                if partition.name in skips:
                    self.log('{} Skipping'.format(partition.name))
                else:
                    self.log("{} Install".format(partition.name))  
                    dest = library.put(partition, force=force)
                    self.log("{} Installed".format(dest[1]))
                    if delete:
                        os.remove(partition.database.path)
                        self.log("{} Deleted".format(partition.database.path))
                    

        return True
        
    def post_install(self):
        from datetime import datetime
        self.db_config.set_value('process', 'installed', datetime.now().isoformat())
        self.library.source.set_bundle_state(self.identity, 'installed')
        return True
    
    ### Submit the package to the repository
 
    def pre_submit(self):
        with self.session:
            self.update_configuration()
        return True
    
    ### Submit the package to the repository
    def submit(self):
    
        self.repository.submit(root=self.run_args.get('name'), force=self.run_args.get('force'),
                               repo=self.run_args.get('repo'))
        return True
    
    def post_submit(self):
        from datetime import datetime
        self.db_config.set_value('process', 'submitted', datetime.now().isoformat())
        return True

    ### Submit the package to the repository
 
    def pre_extract(self):
        return True
    
    ### Submit the package to the repository
    def extract(self):
        self.repository.extract(root=self.run_args.get('name'), force=self.run_args.get('force'))
        return True
    
    def post_extract(self):
        from datetime import datetime
        self.db_config.set_value('process', 'extracted', datetime.now().isoformat())
        return True
    
    
    def repopulate(self):
        '''Pull bundle files from the library back into the working directory'''
        import shutil

        self.log('---- Repopulate ----')
        
        b = self.library.get(self.identity.name)
        
        self.log('Copy bundle from {} to {} '.format(b.database.path, self.database.path))
        
        if not os.path.isdir(os.path.dirname(self.database.path)):
            os.makedirs(os.path.dirname(self.database.path))
            
        shutil.copy(b.database.path, self.database.path)
         
        # Restart with the new bundle database.
        newb = BuildBundle(self.bundle_dir)
        
        for newp in newb.partitions:
            self.log('Copy partition: {}'.format(newp.identity.name))

            b = self.library.get(newp.identity.vname)
            
            dir_ = os.path.dirname(newp.database.path);
            
            if not os.path.isdir(dir_):
                os.makedirs(dir_)
            
            shutil.copy(b.partition.database.path, newp.database.path)

    def set_args(self,args):

        from ..util import AttrDict

        self.run_args = AttrDict(vars(args))


    def run_mp(self, method, arg_sets):
        from ..run import mp_run
        from multiprocessing import Pool, cpu_count

        if len(arg_sets) == 0:
            return

        # Argsets should be tuples, but for one ag functions, the
        # caller may pass in a scalar, which we have to convert.
        if not isinstance(arg_sets[0], (list, tuple)):
            arg_sets = ( (arg,) for arg in arg_sets )

        n = int(self.run_args.get('multi'))

        if n == 0:
            n = cpu_count()


        if n == 1:
            self.log("Requested MP run, but for only 1 process; running in process instead")
            for args in arg_sets:
                method(*args)
        else:
            self.log("Multi processor run with {} processes".format(n))

            pool = Pool(n)

            pool.map(mp_run,[ (self.bundle_dir, method.__name__, args)
                             for args in arg_sets])


    
