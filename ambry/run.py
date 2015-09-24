"""Runtime configuration logic for running a bundle build.

Copyright (c) 2013 Clarinova. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

import copy
import os.path
import sys
import traceback

from six import StringIO, string_types, print_

from ambry.util import AttrDict, lru_cache
from .dbexceptions import ConfigurationError
from .util import parse_url_to_dict, unparse_url_dict


@lru_cache()
def get_runconfig(path=None):
    return RunConfig(path)


class RunConfig(object):
    """Runtime configuration object.

    The RunConfig object will search for a ambry.yaml file in multiple locations
    including::

      /etc/ambry.yaml
      ~user/.ambry.yaml
      ./ambry.yaml
      A named path ( --config option )

    It will start from the first directory, and for each one, try to load the
    file and copy the values into an accumulator, with later values overwritting
    earlier ones.

    """

    # Name of the evironmental var for the config file.
    AMBRY_CONFIG_ENV_VAR = 'AMBRY_CONFIG'
    AMBRY_ACCT_ENV_VAR = 'AMBRY_ACCOUNTS'

    ROOT_CONFIG = '/etc/ambry.yaml'
    USER_CONFIG = (os.getenv(AMBRY_CONFIG_ENV_VAR)
                   if os.getenv(AMBRY_CONFIG_ENV_VAR) else os.path.expanduser('~/.ambry.yaml'))

    pjoin = os.path.join  # Shortcut for simplification.

    # A special case for virtual environments -- look for a user config file there first.
    if os.getenv(AMBRY_CONFIG_ENV_VAR):
        USER_CONFIG = os.getenv(AMBRY_CONFIG_ENV_VAR)
    elif os.getenv('VIRTUAL_ENV') and os.path.exists(pjoin(os.getenv('VIRTUAL_ENV'), '.ambry.yaml')):
        USER_CONFIG = pjoin(os.getenv('VIRTUAL_ENV'), '.ambry.yaml')
    else:
        USER_CONFIG = os.path.expanduser('~/.ambry.yaml')

    if os.getenv(AMBRY_ACCT_ENV_VAR):
        USER_ACCOUNTS = os.getenv(AMBRY_ACCT_ENV_VAR)
    elif os.getenv('VIRTUAL_ENV') and os.path.exists(pjoin(os.getenv('VIRTUAL_ENV'), '.ambry-accounts.yaml')):
        USER_ACCOUNTS = pjoin(os.getenv('VIRTUAL_ENV'), '.ambry-accounts.yaml')
    else:
        USER_ACCOUNTS = os.path.expanduser('~/.ambry-accounts.yaml')

    try:
        DIR_CONFIG = pjoin(os.getcwd(), 'ambry.yaml')  # In webservers, there is no cwd
    except OSError:
        DIR_CONFIG = None

    config = None
    files = None

    def __init__(self, path=None):
        """Create a new RunConfig object.

        Arguments
        path -- If present, a yaml file to load last, overwriting earlier values
          If it is an array, load only the files in the array.

        """

        config = AttrDict()
        config['loaded'] = []

        if not path:
            pass

        if isinstance(path, (list, tuple, set)):
            files = path
        else:
            files = [
                RunConfig.ROOT_CONFIG,
                path if path else RunConfig.USER_CONFIG,
                RunConfig.DIR_CONFIG]

        loaded = False

        for f in files:
            if f is not None and os.path.exists(f):
                try:
                    config.loaded.append(f)
                    config.update_yaml(f)
                    loaded = True
                except TypeError:
                    pass  # Empty files will produce a type error

        if not loaded:
            raise ConfigurationError("Failed to load any config from: {}".format(files))
        

        if os.path.exists(RunConfig.USER_ACCOUNTS):
            config.loaded.append(RunConfig.USER_ACCOUNTS)
            config.update_yaml(RunConfig.USER_ACCOUNTS)

        object.__setattr__(self, 'config', config)
        object.__setattr__(self, 'files', files)

    def __getattr__(self, group):
        """Fetch a configuration group and return the contents as an
        attribute-accessible dict"""

        return self.config.get(group, {})

    def __setattr__(self, group, v):
        """Fetch a configuration group and return the contents as an
        attribute-accessible dict"""

        self.config[group] = v

    def get(self, k, default=None):

        if not default:
            default = None

        return self.config.get(k, default)

    def group(self, name):
        """return a dict for a group of configuration items."""

        if name not in self.config:
            raise ConfigurationError(
                "No group '{}' in configuration.\n"
                "Config has: {}\nLoaded: {}".format(name, list(self.config.keys()), self.loaded))

        return self.config.get(name, {})

    def group_item(self, group, name):

        g = self.group(group)

        if name not in g:
            raise ConfigurationError(
                "Could not find name '{}' in group '{}'. \n"
                "Config has: {}\nLoaded: {}".format(name, group, list(g.keys()), self.loaded))

        return copy.deepcopy(g[name])

    def _yield_string(self, e):
        """Recursively descend a data structure to find string values.

        This will locate values that should be expanded by reference.

        """
        from .util import walk_dict

        for path, subdicts, values in walk_dict(e):
            for k, v in values:

                if v is None:
                    continue

                path_parts = path.split('/')
                path_parts.pop()
                path_parts.pop(0)
                path_parts.append(k)

                def setter(nv):
                    sd = e
                    for pp in path_parts:
                        if not isinstance(sd[pp], dict):
                            break
                        sd = sd[pp]

                    # Save the Original value as a name

                    sd[pp] = nv

                    if isinstance(sd[pp], dict):
                        sd[pp]['_name'] = v

                yield k, v, setter

    def _sub_strings(self, e, subs):
        """Substitute keys in the dict e with functions defined in subs."""

        iters = 0
        while iters < 100:
            sub_count = 0

            for k, v, setter in self._yield_string(e):

                if k in subs:
                    setter(subs[k](k, v))
                    sub_count += 1

            if sub_count == 0:
                break

            iters += 1

        return e

    def dump(self, stream=None):

        to_string = False
        if stream is None:
            stream = StringIO()
            to_string = True

        self.config.dump(stream)

        if to_string:
            stream.seek(0)
            return stream.read()
        else:
            return stream

    def filesystem(self, name, missing_is_dir=False):

        fs = self.group('filesystem')

        e = self.group_item('filesystem', name)

        # Substititue in any of the other items in the filesystem group.
        # this is particularly useful for the 'root' value
        return e.format(**fs)

    def service(self, name):
        """For configuring the client side of services."""

        e = self.group_item('services', name)

        # If the value is a string, rather than a dict, it is for a
        # FsCache. Re-write it to be the expected type.

        if isinstance(e, string_types):
            e = parse_url_to_dict(e)

        if e.get('url', False):
            e.update(parse_url_to_dict(e['url']))

        hn = e.get('hostname', e.get('host', None))

        try:
            account = self.account(hn)
            e['account'] = account
            e['password'] = account.get('password', e['password'])
            e['username'] = account.get('username', e['username'])
        except ConfigurationError:
            e['account'] = None

        e['hostname'] = e['host'] = hn

        e['url'] = unparse_url_dict(e)

        return e

    def servers(self, name, default=None):
        """For configuring the server side of services."""

        try:
            e = self.group_item('servers', name)
        except ConfigurationError:
            if not default:
                raise
            e = default

        # If the value is a string, rather than a dict, it is for a
        # FsCache. Re-write it to be the expected type.

        try:
            account = self.account(e['host'])
            e['account'] = account
            e['password'] = account.get('password', e['password'])
            e['username'] = account.get('username', e['username'])
        except ConfigurationError:
            e['account'] = None

        return e

    def account(self, name):

        e = self.group_item('accounts', name)

        e = self._sub_strings(e, {'store': lambda k, v: self.filesystem(v)})

        e['_name'] = name

        return e

    def remotes(self, remotes):
        # Re-format the string remotes from strings to dicts.

        fs = self.group('filesystem')
        root_dir = fs['root'] if 'root' in fs else '/tmp/norootdir'

        r = {}

        try:
            pairs = list(remotes.items())
        except AttributeError:
            pairs = list(enumerate(remotes))

        for name, remote in pairs:

            remote = remote.format(root=root_dir)

            r[str(name)] = remote

        return r

    def library(self):
        e = self.config['library']

        fs = self.group('filesystem')

        database = e.get('database', '').format(**fs)
        warehouse = e.get('warehouse', '').format(**fs),

        try:
            database = self.database(database, missing_is_dsn=True, return_dsn=True)
        except:
            raise

        try:
            warehouse = self.database(warehouse, missing_is_dsn=True, return_dsn=True)
        except:
            pass

        d = dict(
            database=database,
            warehouse=warehouse,
            remotes=self.remotes(e.get('remotes', {})))

        return d

    def warehouse(self, name):
        from .warehouse import database_config

        e = self.group_item('warehouse', name)

        # The warehouse can be specified as a single database string.
        if isinstance(e, string_types):
            return database_config(e)

        else:

            e = self._sub_strings(e, {
                'account': lambda k, v: self.account(v),
                'library': lambda k, v: self.database(v),
            })

            if 'database' in e and isinstance(e['database'], string_types):
                e.update(database_config(e['database']))

        return e

    def database(self, name, missing_is_dsn=False, return_dsn=False):

        fs = self.group('filesystem')
        root_dir = fs['root'] if 'root' in fs else '/tmp/norootdir'

        try:
            e = self.group_item('database', name)
        except ConfigurationError:
            if missing_is_dsn:
                e = name.format(root=root_dir.rstrip('/'))
            else:
                raise

        # If the value is a string rather than a dict, it is a DSN string

        try:
            e = e.to_dict()
        except AttributeError:
            pass  # Already a dict b/c converted from string

        config, dsn = normalize_dsn_or_dict(e)

        if config.get('server') and not config.get('password'):

            account = None
            fails = []
            account_templates = (
                '{server}-{username}-{dbname}',
                '{server}-{dbname}',
                '{server}-{username}',
                '{server}')
            for tmpl in account_templates:
                try:
                    account_key = tmpl.format(**config)
                    account = self.account(account_key)
                    if account:
                        break
                except KeyError:
                    pass
                except ConfigurationError as exc:
                    fails.append((account_key, str(exc)))

            #for fail in fails:
            #    print fail

            if account:
                config.update(account)

            config, dsn = normalize_dsn_or_dict(config)

        if return_dsn:
            return dsn
        else:
            return config

    @property
    def dict(self):
        return self.config.to_dict()


def mp_run(mp_run_args):
    """ Run a bundle in a multi-processor child process. """

    # FIXME: Seems unused. Remove if so.
    bundle_dir, run_args, method_name, args = mp_run_args

    try:

        # bundle_file = sys.argv[1]

        if not os.path.exists(os.path.join(os.getcwd(), 'bundle.yaml')):
            error_msg = "ERROR: Current directory '{}' does not have a bundle.yaml file, "\
                "so it isn't a bundle file. Did you mean to run 'cli'?"\
                .format(os.getcwd())
            print_(error_msg, file=sys.stderr)
            sys.exit(1)

        # Import the bundle file from the
        rp = os.path.realpath(os.path.join(bundle_dir, 'bundle.py'))
        # FIXME: What is import_file?
        mod = import_file(rp)

        dir_ = os.path.dirname(rp)
        b = mod.Bundle(dir_)
        b.run_args = AttrDict(run_args)

        method = getattr(b, method_name)

        b.log(
            "MP Run: pid={} {}{} ".format(
                os.getpid(),
                method.__name__,
                args))

        try:
            # This close is really important; the child process can't be allowed to use the database
            # connection created by the parent; you get horrible breakages in
            # random places.
            b.close()
            method(*args)
        except:
            b.close()
            raise

    except:
        tb = traceback.format_exc()
        print('==========vvv MP Run Exception: {} pid = {} ==========='.format(args, os.getpid()))
        print(tb)
        print('==========^^^ MP Run Exception: {} pid = {} ==========='.format(args, os.getpid()))
        raise


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
