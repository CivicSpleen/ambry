__author__ = 'eric'

import os
from ..run import get_runconfig

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
        for k,v in parent.dataset.dict.items():
            self['identity'][k] = v

        for row in parent.database.session.query(SAConfig).all():
            if row.group not in self:
                self[row.group] = {}

            self[row.group][row.key] = row.value


class BundleConfig(object):

    def __init__(self):
        pass


class BundleFileConfig(BundleConfig):
    '''Bundle configuration from a bundle.yaml file '''

    BUNDLE_CONFIG_FILE = 'bundle.yaml'

    def __init__(self, root_dir):
        '''Load the bundle.yaml file and create a config object

        If the 'id' value is not set in the yaml file, it will be created and the
        file will be re-written
        '''
        from ..dbexceptions import ConfigurationError

        super(BundleFileConfig, self).__init__()

        self.root_dir = root_dir
        self.local_file = os.path.join(self.root_dir,'bundle.yaml')
        self.source_ref = self.local_file
        self._run_config = get_runconfig(self.local_file)

        # If there is no id field, create it immediately and
        # write the configuration back out.

        if not self._run_config.identity.get('id',False):
            self.init_dataset_number()

        if not os.path.exists(self.local_file):
            raise ConfigurationError("Can't find bundle config file: ")

    def init_dataset_number(self):
        from ambry.identity import Identity, DatasetNumber, NumberServer

        try:
            ns = NumberServer(**self._run_config.group('numbers'))
            ds = ns.next()
        except Exception as e:
            from ..util import get_logger
            logger = get_logger(__name__)

            logger.error("Failed to get number from number sever; need to use self assigned number: {}"
                .format(e.message))
            raise

        ident = Identity.from_dict(self._run_config.identity)

        ident._on = ds.rev(self._run_config.identity.revision)

        self.rewrite(**dict(
            identity=ident.ident_dict,
            names=ident.names_dict
        ))
        self._run_config = get_runconfig(self.local_file)

    @property
    def config(self): #@ReservedAssignment
        '''Return a dict/array object tree for the bundle configuration'''

        return self._run_config

    @property
    def path(self):
        return os.path.join(self.cache, BundleFileConfig.BUNDLE_CONFIG_FILE)

    def get_identity(self):
        '''Return an identity object. '''
        from ..identity import Identity, Name, ObjectNumber

        names = self.names.items()
        idents = self.identity.items()

        return Identity.from_dict(dict(names + idents))

    def rewrite(self, **kwargs):
        '''Re-writes the file from its own data. Reformats it, and updates
        the modification time. Will also look for a config directory and copy the
        contents of files there into the bundle.yaml file, adding a key derived from the name
        of the file. '''

        temp = self.local_file+".temp"
        old = self.local_file+".old"

        config = AttrDict()

        config.update_yaml(self.local_file)


        for k,v in kwargs.items():
            config[k] = v

        with open(temp, 'w') as f:
            config.dump(f)

        if os.path.exists(temp):
            os.rename(self.local_file, old)
            os.rename(temp,self.local_file )

        self._run_config = get_runconfig(self.local_file)

    def dump(self):
        '''Re-writes the file from its own data. Reformats it, and updates
        the modification time'''
        import yaml

        return yaml.dump(self._run_config, indent=4, default_flow_style=False)


    def __getattr__(self, group):
        '''Fetch a confiration group and return the contents as an
        attribute-accessible dict'''
        return self._run_config.group(group)

    def group(self, name):
        '''return a dict for a group of configuration items.'''

        return self._run_config.group(name)

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
        self.dataset = bundle.dataset # (self.database.session.query(Dataset).one())

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
                                  SAConfig.d_vid == self.dataset.vid).delete()


        o = SAConfig(group=group, key=key,d_vid=self.dataset.vid,value = value)
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





