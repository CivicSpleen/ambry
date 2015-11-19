"""Runtime configuration logic for running a bundle build.

Copyright (c) 2013 Clarinova. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

import copy
import os.path

from six import StringIO, string_types

from ambry.util import AttrDict, lru_cache, parse_url_to_dict, unparse_url_dict
from .dbexceptions import ConfigurationError


@lru_cache()
def get_runconfig(path=None):
    return load(path)

from ambry.util import Constant

ENVAR = Constant()
ENVAR.CONFIG = 'AMBRY_CONFIG'
ENVAR.ACCT = 'AMBRY_ACCOUNTS'
ENVAR.PASSWORD = 'AMBRY_ACCOUNT_PASSWORD'
ENVAR.DB =  'AMBRY_DB'
ENVAR.ROOT =  'AMBRY_ROOT'
ENVAR.EDIT = 'AMBRY_CONFIG_EDIT'
ENVAR.VIRT = 'VIRTUAL_ENV'

filesystem_defaults = {
    'build': '{root}/build',
    'documentation': '{root}/doc',
    'downloads': '{root}/downloads',
    'extracts': '{root}/extracts',
    'logs': '{root}/logs',
    'python': '{root}/python',
    'search': '{root}/search',
    'source': '{root}/source',
    'test': '{root}/test',
}

def load_accounts():
    """Load one Yaml file of account information.

    :return: An `AttrDict`
    """
    from os.path import join
    from os.path import getmtime

    config = AttrDict()

    if os.getenv(ENVAR.ACCT):
        accts_file = os.getenv(ENVAR.ACCT)

    elif os.getenv(ENVAR.VIRT) and os.path.exists(join(os.getenv(ENVAR.VIRT), '.ambry-accounts.yaml')):
        accts_file = join(os.getenv(ENVAR.VIRT), '.ambry-accounts.yaml')

    else:
        accts_file = os.path.expanduser('~/.ambry-accounts.yaml')

    if os.path.exists(accts_file):
        config.update_yaml(accts_file)

        config.accounts.loaded = [accts_file, getmtime(accts_file)]

    else:
        config.accounts = AttrDict()
        config.accounts.loaded = [None, 0]



    return config

def load_config(path=None):
    """
    Load configuration information from one or more files. Tries to load from, in this order:

    - /etc/ambry.yaml
    - ~/.ambry.yaml
    - A path specified by the AMBRY_CONFIG environmenal variable
    - .ambry.yaml in the directory specified by the VIRTUAL_ENV environmental variable
    - .ambry.yaml in the current working directory


    :param path: An iterable of additional paths to load.
    :return: An `AttrDict` of configuration information
    """

    from os.path import join
    from os.path import getmtime

    config = AttrDict()


    files = []

    if not path:
        files.append('/etc/ambry.yaml')

        files.append(os.path.expanduser('~/.ambry.yaml'))

        if os.getenv(ENVAR.CONFIG):
            files.append(os.getenv(ENVAR.CONFIG))

        if os.getenv(ENVAR.VIRT):
            files.append(join(os.getenv('VIRTUAL_ENV'), '.ambry.yaml'))

        files.append(join(os.getenv(ENVAR.VIRT), '.ambry.yaml'))

        try:
            files.append(join(os.getcwd(), 'ambry.yaml'))
        except OSError:
            pass # In webservers, there is no cwd

    if isinstance(path, (list, tuple, set)):
        for p in path:
            files.append(p)
    else:
        files.append(path)

    files = list(set(files))
    loaded = []

    for f in files:
        if f is not None and os.path.exists(f):

            try:
                config.update_yaml(f)
                loaded.append((f,getmtime(f) ))

            except TypeError:
                pass  # Empty files will produce a type error

    #if not config:
    #    raise ConfigurationError("Failed to load any config from: {}".format(files))

    config.loaded = loaded

    return config

def update_config(config):
    """Update the configuration from environmental variables. Updates:

    - config.library.database from the AMBRY_DB environmental variable.
    - config.library.filesystem_root from the AMBRY_ROOT environmental variable.
    - config.accounts.password from the AMBRY_PASSWORD  environmental variable.

    :param config: An `attrDict` of configuration information.
    """


    try:
        _ = config.accounts
    except KeyError:
        config.accounts = AttrDict()

    try:
        _ = config.library
    except KeyError:
        config.library = AttrDict()

    try:
        _ = config.filesystem
    except KeyError:
        config.filesystem = AttrDict()

    try:
        _ = config.accounts.password
    except KeyError:
        config.accounts.password = None

    if os.getenv(ENVAR.DB):
        config.library.database = os.getenv(ENVAR.DB)

    if os.getenv(ENVAR.ROOT):
        config.library.filesystem_root = os.getenv(ENVAR.ROOT)

    if os.getenv(ENVAR.PASSWORD):
        config.accounts.password = os.getenv(ENVAR.PASSWORD)

    try:
        _ = config.library.remotes
    except KeyError:
        config.library.remotes = AttrDict() # Default empty

    # Set a default for the library database
    try:
        _ = config.library.database
    except KeyError:
        config.library.database = 'sqlite:///{root}/library.db'

    # Raise exceptions on missing items
    checks = [
        'config.library.filesystem_root',
    ]

    for check in checks:
        try:
            _ = eval(check)
        except KeyError:
            raise ConfigurationError("Configuration is missing '{}'; loaded from {} "
                                     .format(check, [l[0] for l in config.loaded]))

    _, config.library.database =  normalize_dsn_or_dict(config.library.database)

    for k, v in filesystem_defaults.items():
        if k not in config.filesystem:
            config.filesystem[k] = v

    config.modtime = max([l[1] for l in  config.loaded ] + [config.accounts.loaded[1]])


def load(path = None):

    config = load_config(path)
    config.update(load_accounts())

    update_config(config)

    return config




def normalize_dsn_or_dict(d):

    if isinstance(d, dict):

        try:
            # Convert from an AttrDict to a real dict
            d = d.to_dict()
        except AttributeError:
            pass  # Already a real dict

        config = d
        dsn = None

    elif isinstance(d, string_types):
        config = None
        dsn = d

    else:
        raise ConfigurationError("Can't deal with database config '{}' type '{}' ".format(d, type(d)))

    if dsn:

        if dsn.startswith('sqlite') or dsn.startswith('spatialite'):
            driver, path = dsn.split(':', 1)

            slashes, path = path[:2], path[2:]

            if slashes != '//':
                raise ConfigurationError("Sqlite DSNs must start with at least 2 slashes")

            if len(path) == 1 and path[0] == '/':
                raise ConfigurationError("Sqlite DSNs can't have only 3 slashes in path")

            if len(path) > 1 and path[0] != '/':
                raise ConfigurationError("Sqlite DSNs with a path must have 3 or 4 slashes.")

            path = path[1:]

            config = dict(
                server=None,
                username=None,
                password=None,
                driver=driver,
                dbname=path
            )
        else:

            d = parse_url_to_dict(dsn)

            config = dict(
                server=d['hostname'],
                dbname=d['path'].strip('/'),
                driver=d['scheme'],
                password=d.get('password', None),
                username=d.get('username', None)
            )

    else:
        
        up = d.get('username', '') or ''

        if d.get('password'):

            up += ':' + d.get('password', '')

        if up:
            up += "@"

        if up and not d.get('server'):
            raise ConfigurationError("Can't construct a DSN with a username or password without a hostname")

        host_part = up + d.get('server', '') if d.get('server') else ''

        if d.get('dbname', False):
            path_part = '/' + d.get('dbname')

            #if d['driver'] in ('sqlite3', 'sqlite', 'spatialite'):
            #    path_part = '/' + path_part

        else:
            path_part = '' # w/ no dbname, Sqlite should use memory, which required 2 slash. Rel dir is 3, abs dir is 4

        dsn = "{}://{}{}".format(d['driver'], host_part, path_part)

    return config, dsn
