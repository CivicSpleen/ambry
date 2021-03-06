"""Runtime configuration logic for running a bundle build.

Copyright (c) 2013 Clarinova. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

import os.path

from six import string_types

from ambry.util import AttrDict, parse_url_to_dict
from .dbexceptions import ConfigurationError


def get_runconfig(path=None, root=None, db=None):
    """Load the main configuration files and accounts file.

    Debprecated. Use load()
    """

    return load(path, root=root, db=db)


def load(path=None, root=None, db=None, load_user=True):
    "Load all of the config files. "

    config = load_config(path, load_user=load_user)

    remotes = load_remotes(path, load_user=load_user)

    # The external file overwrites the main config
    if remotes:
        if not 'remotes' in config:
            config.remotes = AttrDict()

        for k, v in remotes.remotes.items():
            config.remotes[k] = v

    accounts = load_accounts(path, load_user=load_user)

    # The external file overwrites the main config
    if accounts:
        if not 'accounts' in config:
            config.accounts = AttrDict()
        for k, v in accounts.accounts.items():
            config.accounts[k] = v

    update_config(config)

    if root:
        config.library.filesystem_root = root

    if db:
        config.library.database = db

    return config

from ambry.util import Constant

ENVAR = Constant()
ENVAR.CONFIG = 'AMBRY_CONFIG'
ENVAR.PASSWORD = 'AMBRY_ACCOUNT_PASSWORD'
ENVAR.DB = 'AMBRY_DB'
ENVAR.ROOT = 'AMBRY_ROOT'
ENVAR.EDIT = 'AMBRY_CONFIG_EDIT'
ENVAR.VIRT = 'VIRTUAL_ENV'

ROOT_DIR = '/etc/ambry'
USER_DIR = '.ambry'
CONFIG_FILE = 'config.yaml'
ACCOUNTS_FILE = 'accounts.yaml'
REMOTES_FILE = 'remotes.yaml'
DOCKER_FILE = 'docker.yaml'

filesystem_defaults = {
    'build': '{root}/build',
    'documentation': '{root}/doc',
    'downloads': '{root}/downloads',
    'cache': '{root}/cache',
    'extracts': '{root}/extracts',
    'logs': '{root}/logs',
    'python': '{root}/python',
    'search': '{root}/search',
    'source': '{root}/source',
    'test': '{root}/test',
}


def find_config_file(file_name, extra_path=None, load_user=True):
    """
    Find a configuration file in one of these directories, tried in this order:

    - A path provided as an argument
    - A path specified by the AMBRY_CONFIG environmenal variable
    - ambry in a path specified by the VIRTUAL_ENV environmental variable
    - ~/ambry
    - /etc/ambry

    :param file_name:
    :param extra_path:
    :param load_user:
    :param path:
    :return:
    """

    paths = []

    if extra_path is not None:
        paths.append(extra_path)

    if os.getenv(ENVAR.CONFIG):
        paths.append(os.getenv(ENVAR.CONFIG))

    if os.getenv(ENVAR.VIRT):
        paths.append(os.path.join(os.getenv(ENVAR.VIRT), USER_DIR))

    if load_user:
        paths.append(os.path.expanduser('~/' + USER_DIR))

    paths.append(ROOT_DIR)

    for path in paths:
        if os.path.isdir(path) and os.path.exists(os.path.join(path, file_name)):
            f = os.path.join(path, file_name)
            return f

    raise ConfigurationError(
        "Failed to find configuration file '{}'. Looked for : {} ".format(file_name, paths))


def load_accounts(extra_path=None, load_user=True):
    """Load the yaml account files

    :param load_user:
    :return: An `AttrDict`
    """

    from os.path import getmtime


    try:
        accts_file = find_config_file(ACCOUNTS_FILE, extra_path=extra_path, load_user=load_user)
    except ConfigurationError:
        accts_file = None

    if accts_file is not None and os.path.exists(accts_file):
        config = AttrDict()
        config.update_yaml(accts_file)

        if not 'accounts' in config:
            config.remotes = AttrDict()

        config.accounts.loaded = [accts_file, getmtime(accts_file)]
        return config
    else:
        return None


def load_remotes(extra_path=None, load_user=True):
    """Load the YAML remotes file, which sort of combines the Accounts file with part of the
    remotes sections from the main config

    :return: An `AttrDict`
    """

    from os.path import getmtime

    try:
        remotes_file = find_config_file(REMOTES_FILE, extra_path=extra_path, load_user=load_user)
    except ConfigurationError:
        remotes_file = None


    if remotes_file is not None and os.path.exists(remotes_file):
        config = AttrDict()
        config.update_yaml(remotes_file)

        if not 'remotes' in config:
            config.remotes = AttrDict()

        config.remotes.loaded = [remotes_file, getmtime(remotes_file)]

        return config
    else:
        return None


def load_config(path=None, load_user=True):
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

    from os.path import getmtime

    config = AttrDict()

    if not path:
        path = ROOT_DIR

    config_file = find_config_file(CONFIG_FILE, extra_path=path, load_user=load_user)

    if os.path.exists(config_file):

        config.update_yaml(config_file)
        config.loaded = [config_file, getmtime(config_file)]

    else:
        # Probably never get here, since the find_config_dir would have thrown a ConfigurationError
        config = AttrDict()
        config.loaded = [None, 0]

    return config


def update_config(config, use_environ=True):
    """Update the configuration from environmental variables. Updates:

    - config.library.database from the AMBRY_DB environmental variable.
    - config.library.filesystem_root from the AMBRY_ROOT environmental variable.
    - config.accounts.password from the AMBRY_PASSWORD  environmental variable.

    :param config: An `attrDict` of configuration information.
    """
    from ambry.util import select_from_url


    try:
        _ = config.library
    except KeyError:
        config.library = AttrDict()

    try:
        _ = config.filesystem
    except KeyError:
        config.filesystem = AttrDict()

    try:
        _ = config.accounts
    except KeyError:
        config.accounts = AttrDict()

    if not config.accounts.get('loaded'):
        config.accounts.loaded = [None, 0]

    try:
        _ = config.accounts.password
    except KeyError:
        config.accounts.password = None

    try:
        _ = config.remotes
    except KeyError:
        config.remotes = AttrDict()  # Default empty

    if not config.remotes.get('loaded'):
        config.remotes.loaded = [None, 0]

    if use_environ:
        if os.getenv(ENVAR.DB):
            config.library.database = os.getenv(ENVAR.DB)

        if os.getenv(ENVAR.ROOT):
            config.library.filesystem_root = os.getenv(ENVAR.ROOT)

        if os.getenv(ENVAR.PASSWORD):
            config.accounts.password = os.getenv(ENVAR.PASSWORD)

    # Move any remotes that were configured under the library to the remotes section

    try:
        for k, v in config.library.remotes.items():
            config.remotes[k] = {
                'url': v
            }

        del config.library['remotes']

    except KeyError as e:
        pass

    # Then move any of the account entries that are linked to remotes into the remotes.

    try:
        for k, v in config.remotes.items():
            if 'url' in v:
                host = select_from_url(v['url'], 'netloc')
                if host in config.accounts:
                    config.remotes[k].update(config.accounts[host])
                    del config.accounts[host]

    except KeyError:
        pass


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
                                     .format(check, config.loaded[0]))

    _, config.library.database = normalize_dsn_or_dict(config.library.database)

    for k, v in filesystem_defaults.items():
        if k not in config.filesystem:
            config.filesystem[k] = v

    config.modtime = max(config.loaded[1], config.remotes.loaded[1], config.accounts.loaded[1])


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
            up += '@'

        if up and not d.get('server'):
            raise ConfigurationError("Can't construct a DSN with a username or password without a hostname")

        host_part = up + d.get('server', '') if d.get('server') else ''

        if d.get('dbname', False):
            path_part = '/' + d.get('dbname')

            # if d['driver'] in ('sqlite3', 'sqlite', 'spatialite'):
            #     path_part = '/' + path_part

        else:
            path_part = ''  # w/ no dbname, Sqlite should use memory, which required 2 slash. Rel dir is 3, abs dir is 4

        dsn = '{}://{}{}'.format(d['driver'], host_part, path_part)

    return config, dsn
