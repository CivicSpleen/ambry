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
    """Load the main configuration files and accounts file.

    Load the main configuration files and accounts file from a variety of potential places,
    and update the configuration with values from environmental variables.

    The routine attempts to load the configuration from these locations, in this order:

    - /etc/ambry.yaml
    - ~/.ambry.yaml
    - A path specified by the AMBRY_CONFIG environmenal variable
    - .ambry.yaml in the directory specified by the VIRTUAL_ENV environmental variable
    - .ambry.yaml in the current working directory

    Or, if `path` is specified, it load that path and ignores the standard locations.

    The routines also loads the accounts file from one of these locations:

    - A path specified by the AMBRY_ACCOUNTS environmenal variable
    - .ambry-accounts.yaml in the directory specified by the VIRTUAL_ENV environmental variable
    - ~/.ambry-accounts.yaml

    After loading the configuation, the config is updated from environmental variables:

    - `config.library.database` from `AMBRY_DB`
    - `config.library.filesystem_root` from `AMBRY_ROOT`
    - `config.accounts.password` from `AMBRY_PASSWORD`

    The config file can be empty or missing if  the config has already been loaded into the database, and the
    database, root and password are specified in environmental variables.

    The typical use cases are that for a single machine installation, or the head of a multi-machine installation,
    there is a full configuation file and accounts file. In a multi-machine installation, on;y the environmental
    varaibles AMBRY_DB, AMBRY_ROOT and AMBRY_PASSWORD are specified, allowing satellite machines to get information
    about remotes and accounts from the database. Since the filesystem entries can be taken from the defaults,
    the satellite machines  do not need a configuration file.

    :param path: The path to a configuration file to use instead of the standard locataions.
    """

    return load(path)

def load(path = None):

    config = load_config(path)
    config.update(load_accounts())

    update_config(config)

    return config

from ambry.util import Constant

ENVAR = Constant()
ENVAR.CONFIG = 'AMBRY_CONFIG'
ENVAR.PASSWORD = 'AMBRY_ACCOUNT_PASSWORD'
ENVAR.DB =  'AMBRY_DB'
ENVAR.ROOT =  'AMBRY_ROOT'
ENVAR.EDIT = 'AMBRY_CONFIG_EDIT'
ENVAR.VIRT = 'VIRTUAL_ENV'

ROOT_DIR = '/etc/ambry'
USER_DIR = '~/ambry'
CONFIG_FILE = 'config.yaml'
ACCOUNTS_FILE = 'accounts.yaml'

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

def find_config_dir(extra_path=None):
    """
    Find a configuration directory Tries directories in this order:

    - A path provided as an argument
    - A path specified by the AMBRY_CONFIG environmenal variable
    - ambry in a path specified by the VIRTUAL_ENV environmental variable
    - ~/ambry
    - /etc/ambry


    To be considered valid, the directory must contain a config.yaml file

    :param path:
    :return:
    """

    paths = []

    if extra_path is not None:
        paths.append(extra_paths)

    if os.getenv(ENVAR.CONFIG):
        paths.append(os.getenv(ENVAR.CONFIG))

    if os.getenv(ENVAR.VIRT):
        paths.append(os.getenv(ENVAR.VIRT))

    paths += [
        os.path.expanduser(USER_DIR),
        ROOT_DIR
    ]

    for path in paths:
        if os.path.isdir(path) and os.path.exists(os.path.join(path, CONFIG_FILE)):
            return path

    raise ConfigurationError('Failed to find configuration directory. Looked for : {} '.format(paths))

def load_accounts():
    """Load the yaml account files

    :return: An `AttrDict`
    """

    from os.path import join
    from os.path import getmtime

    config = AttrDict()

    accts_file = os.path.join(find_config_dir(), ACCOUNTS_FILE)

    if os.path.exists(accts_file):
        config.update_yaml(accts_file)
        config.accounts.loaded = [accts_file, getmtime(accts_file)]

    else:
        config.accounts = AttrDict()
        config.accounts.loaded = [None, 0]


    return config



def load_config(path=None):
    """
    Load configuration information from a config directory. Tries directories in this order:

    - A path provided as an argument
    - A path specified by the AMBRY_CONFIG environmenal variable
    - ambry in a path specified by the VIRTUAL_ENV environmental variable
    - /etc/ambry
    - ~/ambry


    :param path: An iterable of additional paths to load.
    :return: An `AttrDict` of configuration information
    """

    from os.path import join
    from os.path import getmtime

    config = AttrDict()

    config_file = os.path.join(find_config_dir(), CONFIG_FILE)

    if os.path.exists(config_file):
        config.update_yaml(config_file)
        config.loaded = [config_file, getmtime(config_file)]

    else:
        # Probably never get here, since the find_config_dir would have thrown a ConfigurationError
        config = AttrDict()
        config.loaded = [None, 0]

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





def normalize_dsn_or_dict(d):
    """Clean up a database DSN, or dict version of a DSN, returning both the cleaned DSN and dict version"""
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
