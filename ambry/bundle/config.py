__author__ = 'eric'

import os
from ..run import get_runconfig, RunConfig

from ambry.run import AttrDict
class BundleDbConfigDict(AttrDict):

    def __init__(self, parent):

        super(BundleDbConfigDict, self).__init__()

        '''load all of the values'''
        from ambry.orm import Config as SAConfig

        for k,v in self.items():
            del self[k]

        # Load the dataset
        self['identity'] = {}

        for k,v in parent.bundle.dataset.dict.items():
            self['identity'][k] = v

        for row in parent.database.session.query(SAConfig).all():
            if row.group not in self:
                self[row.group] = {}

            self[row.group][row.key] = row.value

class BundleConfig(object):

    def __init__(self):
        pass

class BundleDbConfig(BundleConfig):
    ''' Retrieves configuration from the database, rather than the .yaml file. '''

    database = None
    dataset = None

    def __init__(self, bundle, database):
        '''Maintain link between bundle.yam file and Config record in database'''

        super(BundleDbConfig, self).__init__()

        if not database:
            raise Exception("Didn't get database")

        self.bundle = bundle
        self.database = database
        self.source_ref = self.database.dsn


    @property
    def dict(self): #@ReservedAssignment
        '''Return a dict/array object tree for the bundle configuration'''
        from ambry.orm import Config
        from collections import defaultdict

        d = defaultdict(dict)

        for cfg in self.database.session.query(Config).all():

            d[cfg.group][cfg.key] = cfg.value

        return d

    def __getattr__(self, group):
        '''Fetch a confiration group and return the contents as an
        attribute-accessible dict'''

        return self.group(group)

    def group(self, group):
        '''return a dict for a group of configuration items.'''

        bd = BundleDbConfigDict(self)

        group = bd.get(group)

        if not group:
            return None


        return group

    def set_value(self, group, key, value):
        from ambry.orm import Config as SAConfig

        if self.group == 'identity':
            raise ValueError("Can't set identity group from this interface. Use the dataset")

            key = key.strip('_')

        self.database.session.query(SAConfig).filter(SAConfig.group == group,
                                  SAConfig.key == key,
                                  SAConfig.d_vid == self.bundle.dataset.vid).delete()


        o = SAConfig(group=group, key=key,d_vid=self.bundle.dataset.vid,value = value)
        self.database.session.add(o)

    def get_value(self, group, key, default=None):

        group = self.group(group)

        if not group:
            return None

        try:
            return group.__getattr__(key)
        except KeyError:
            if default is not None:
                return default
            raise


    @property
    def partition(self):
        '''Initialize the identity, creating a dataset record,
        from the bundle.yaml file'''

        from ambry.orm import Partition

        return  (self.database.session.query(Partition).first())

class BundleFileConfig(RunConfig):
    '''Bundle configuration from a bundle.yaml file '''

    BUNDLE_CONFIG_FILE = 'bundle.yaml'

    local_groups = [ 'build','about','identity','names','partitions','extracts','views','data', 'visualizations' ]

    def __init__(self,bundle_dir):
        from ..dbexceptions import  ConfigurationError

        object.__setattr__(self, 'bundle_dir', bundle_dir)
        object.__setattr__(self, 'local_file', os.path.join(self.bundle_dir,'bundle.yaml'))

        super(BundleFileConfig, self).__init__(self.local_file)

        if not self.identity.get('id', False):
            self.init_dataset_number()

        if not os.path.exists(self.local_file):
            raise ConfigurationError("Can't find bundle config file: ")

    def __getattr__(self, group):
        '''Fetch a configuration group and return the contents as an
        attribute-accessible dict'''

        if group not in self.config and group in self.local_groups:
            self.config[group] = AttrDict()

        return self.config.get(group,{})


    def get_identity(self):
        '''Return an identity object. '''
        from ..identity import Identity, Name, ObjectNumber

        names = self.names.items()
        idents = self.identity.items()

        return Identity.from_dict(dict(names + idents))

    def dump(self):
        '''Return as a yaml string'''
        import yaml

        return self.config.dump()

    def rewrite(self, **kwargs):
        '''Re-writes the file from its own data. Reformats it, and updates
        the modification time. Will also look for a config directory and copy the
        contents of files there into the bundle.yaml file, adding a key derived from the name
        of the file. '''

        temp = self.local_file + ".temp"
        old = self.local_file + ".old"

        config = AttrDict()

        config.update_yaml(self.local_file)

        # Copy local data out of the whole config
        for k in self.local_groups:
            if k in self.config:

                config[k] = self.config[k]

        # Replace with items from the arg list.
        for k, v in kwargs.items():
            config[k] = v

        with open(temp, 'w') as f:
            config.dump(f)

        if os.path.exists(temp):
            os.rename(self.local_file, old)
            os.rename(temp, self.local_file)

        super(BundleFileConfig, self).__init__(self.local_file)

    def init_dataset_number(self):
        from ambry.identity import Identity, DatasetNumber, NumberServer

        try:
            ns = NumberServer(**self.group('numbers'))
            ds = ns.next()
        except Exception as e:
            from ..util import get_logger

            logger = get_logger(__name__)

            logger.error("Failed to get number from number sever; need to use self assigned number: {}"
                         .format(e.message))
            raise

        self.identity['id'] = str(ds)

        ident = Identity.from_dict(self.identity)

        ident._on = ds.rev(self.identity.revision)

        self.rewrite(**dict(
            identity=ident.ident_dict,
            names=ident.names_dict
        ))

