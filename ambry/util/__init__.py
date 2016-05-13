"""Misc support code.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

from collections import OrderedDict, defaultdict, Mapping, deque, MutableMapping, Callable
from functools import partial, reduce, wraps
import json
import hashlib
import logging

import os
import pprint
import re
import subprocess
import sys
from time import time
import yaml
from yaml.representer import RepresenterError
import warnings

from bs4 import BeautifulSoup

from six.moves import filterfalse, xrange as six_xrange
from six import iteritems, iterkeys, itervalues, print_, StringIO
from six.moves.urllib.parse import urlparse, urlsplit, urlunsplit
from six.moves.urllib.request import urlopen

from ambry.dbexceptions import ConfigurationError

logger_init = set()


def get_logger(name, file_name=None, stream=None, template=None, propagate=False, level=None):
    """Get a logger by name.

    """

    logger = logging.getLogger(name)
    running_tests = (
        'test' in sys.argv  # running with setup.py
        or sys.argv[0].endswith('py.test'))  # running with py.test
    if running_tests and not level:
        # testing without level, this means tester does not want to see any log messages.
        level = logging.CRITICAL

    if not level:
        level = logging.INFO
    logger.setLevel(level)
    logger.propagate = propagate

    formatter = logging.Formatter(template)

    if not stream:
        stream = sys.stdout

    logger.handlers = []
    handler = logging.StreamHandler(stream=stream)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    if file_name:
        handler = logging.FileHandler(file_name)
        handler.setFormatter(logging.Formatter('%(asctime)s '+template))
        logger.addHandler(handler)

    return logger


# From https://wiki.python.org/moin/PythonDecoratorLibrary#Memoize


def memoize(obj):
    cache = obj.cache = {}

    @wraps(obj)
    def memoizer(*args, **kwargs):
        key = str(args) + str(kwargs)
        if key not in cache:
            cache[key] = obj(*args, **kwargs)
        return cache[key]

    return memoizer


def expiring_memoize(obj):
    """Like memoize, but forgets after 10 seconds."""

    cache = obj.cache = {}
    last_access = obj.last_access = defaultdict(int)

    @wraps(obj)
    def memoizer(*args, **kwargs):
        key = str(args) + str(kwargs)

        if last_access[key] and last_access[key] + 10 < time():
            if key in cache:
                del cache[key]

        last_access[key] = time()

        if key not in cache:
            cache[key] = obj(*args, **kwargs)
        return cache[key]

    return memoizer


class Counter(dict):
    """Mapping where default values are zero."""

    def __missing__(self, key):
        return 0


# Stolen from:
# http://code.activestate.com/recipes/498245-lru-and-lfu-cache-decorators/


def lru_cache(maxsize=128, maxtime=60):
    '''Least-recently-used cache decorator.

    Arguments to the cached function must be hashable.
    Cache performance statistics stored in f.hits and f.misses.
    Clear the cache with f.clear().
    http://en.wikipedia.org/wiki/Cache_algorithms#Least_Recently_Used

    '''
    maxqueue = maxsize * 10

    # @ReservedAssignment
    def decorating_function(
            user_function,
            len=len,
            iter=iter,
            tuple=tuple,
            sorted=sorted,
            KeyError=KeyError):

        cache = {}  # mapping of args to results
        queue = deque()  # order that keys have been used
        refcount = Counter()  # times each key is in the queue
        sentinel = object()  # marker for looping around the queue
        kwd_mark = object()  # separate positional and keyword args

        # lookup optimizations (ugly but fast)
        queue_append, queue_popleft = queue.append, queue.popleft
        queue_appendleft, queue_pop = queue.appendleft, queue.pop

        @wraps(user_function)
        def wrapper(*args, **kwds):
            # cache key records both positional and keyword args
            key = args
            if kwds:
                key += (kwd_mark,) + tuple(sorted(kwds.items()))

            # record recent use of this key
            queue_append(key)
            refcount[key] += 1

            # get cache entry or compute if not found
            try:
                result, expire_time = cache[key]

                if expire_time and time() > expire_time:
                    raise KeyError('Expired')

                wrapper.hits += 1
            except KeyError:
                result = user_function(*args, **kwds)
                if maxtime:
                    expire_time = time() + maxtime
                else:
                    expire_time = None

                cache[key] = result, expire_time
                wrapper.misses += 1

                # purge least recently used cache entry
                if len(cache) > maxsize:
                    key = queue_popleft()
                    refcount[key] -= 1
                    while refcount[key]:
                        key = queue_popleft()
                        refcount[key] -= 1
                    del cache[key], refcount[key]

            # periodically compact the queue by eliminating duplicate keys
            # while preserving order of most recent access
            if len(queue) > maxqueue:
                refcount.clear()
                queue_appendleft(sentinel)
                for key in filterfalse(refcount.__contains__, iter(queue_pop, sentinel)):
                    queue_appendleft(key)
                    refcount[key] = 1

            return result

        def clear():
            cache.clear()
            queue.clear()
            refcount.clear()
            wrapper.hits = wrapper.misses = 0

        wrapper.hits = wrapper.misses = 0
        wrapper.clear = clear
        return wrapper

    return decorating_function


class YamlIncludeLoader(yaml.Loader):
    def __init__(self, stream):
        self._root = os.path.split(stream.name)[0]

        super(YamlIncludeLoader, self).__init__(stream)


# From http://pypi.python.org/pypi/layered-yaml-attrdict-config/12.07.1
class OrderedDictYAMLLoader(yaml.Loader):
    'Based on: https://gist.github.com/844388'

    def __init__(self, *args, **kwargs):
        yaml.Loader.__init__(self, *args, **kwargs)

        self.dir = None
        for a in args:
            try:
                self.dir = os.path.dirname(a.name)
            except:
                pass

        self.add_constructor(
            'tag:yaml.org,2002:map',
            type(self).construct_yaml_map)
        self.add_constructor(
            'tag:yaml.org,2002:omap',
            type(self).construct_yaml_map)
        self.add_constructor('!include', OrderedDictYAMLLoader.include)

    def construct_yaml_map(self, node):
        data = OrderedDict()
        yield data
        value = self.construct_mapping(node)
        data.update(value)

    def construct_mapping(self, node, deep=False):
        if isinstance(node, yaml.MappingNode):
            self.flatten_mapping(node)
        else:
            raise yaml.constructor.ConstructorError(
                None,
                None,
                'expected a mapping node, but found {}'.format(
                    node.id),
                node.start_mark)

        mapping = OrderedDict()
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            try:
                hash(key)
            except TypeError as exc:
                raise yaml.constructor.ConstructorError(
                    'while constructing a mapping',
                    node.start_mark,
                    'found unacceptable key ({})'.format(exc),
                    key_node.start_mark)
            value = self.construct_object(value_node, deep=deep)
            mapping[key] = value
        return mapping

    def include(self, node):
        if not self.dir:
            return "ConfigurationError: Can't include file: wasn't able to set base directory"

        relpath = self.construct_scalar(node)
        abspath = os.path.join(self.dir, relpath)

        if not os.path.exists(abspath):
            raise ConfigurationError(
                "Can't include file '{}': Does not exist".format(abspath))

        with open(abspath, 'r') as f:

            parts = abspath.split('.')
            ext = parts.pop()

            if ext == 'yaml':
                return yaml.load(f, OrderedDictYAMLLoader)
            else:
                return IncludeFile(abspath, relpath, f.read())


# IncludeFile and include_representer ensures that when config files are re-written, they are
# represented as an include, not the contents of the include


class IncludeFile(str):
    def __new__(cls, abspath, relpath, data):
        s = str.__new__(cls, data)
        s.abspath = abspath
        s.relpath = relpath
        return s


def include_representer(dumper, data):
    return dumper.represent_scalar('!include', data.relpath)


# http://pypi.python.org/pypi/layered-yaml-attrdict-config/12.07.1
class AttrDict(OrderedDict):
    def __init__(self, *argz, **kwz):
        super(AttrDict, self).__init__(*argz, **kwz)

    def __setitem__(self, k, v):
        super(AttrDict, self).__setitem__(k, AttrDict(v) if isinstance(v, Mapping) else v)

    def __getattr__(self, k):
        if not (k.startswith('__') or k.startswith('_OrderedDict__')):
            return self[k]
        else:
            return super(AttrDict, self).__getattr__(k)

    def __setattr__(self, k, v):
        if k.startswith('_OrderedDict__'):
            return super(AttrDict, self).__setattr__(k, v)
        self[k] = v

    def __iter__(self):
        return iterkeys(super(OrderedDict, self))

    ##
    # __enter__ and __exit__ allow for assigning a  path to a variable
    # with 'with', which isn't extra functionalm but looks pretty.
    ##

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        return False

    @classmethod
    def from_yaml(cls, path, if_exists=False):
        if if_exists and not os.path.exists(path):
            return cls()

        with open(path) as f:
            return cls(yaml.load(f, OrderedDictYAMLLoader) or {})

    @staticmethod
    def flatten_dict(data, path=tuple()):
        dst = list()
        for k, v in iteritems(data):
            k = path + (k,)
            if isinstance(v, Mapping):
                for v in v.flatten(k):
                    dst.append(v)
            else:
                dst.append((k, v))
        return dst

    def flatten(self, path=tuple()):
        return self.flatten_dict(self, path=path)

    def update_flat(self, val):

        if isinstance(val, AttrDict):
            val = val.flatten()

        for k, v in val:
            dst = self
            for slug in k[:-1]:
                if dst.get(slug) is None:
                    dst[slug] = AttrDict()
                dst = dst[slug]
            if v is not None or not isinstance(dst.get(k[-1]), Mapping):
                dst[k[-1]] = v

    def unflatten_row(self, k, v):
        dst = self
        for slug in k[:-1]:

            if slug is None:
                continue

            if dst.get(slug) is None:
                dst[slug] = AttrDict()
            dst = dst[slug]
        if v is not None or not isinstance(dst.get(k[-1]), Mapping):
            dst[k[-1]] = v

    def update_yaml(self, path):
        self.update_flat(self.from_yaml(path))
        return self

    def to_dict(self):
        root = {}
        val = self.flatten()
        for k, v in val:
            dst = root
            for slug in k[:-1]:
                if dst.get(slug) is None:
                    dst[slug] = dict()
                dst = dst[slug]
            if v is not None or not isinstance(dst.get(k[-1]), Mapping):
                dst[k[-1]] = v

        return root

    def update_dict(self, data):
        self.update_flat(self.flatten_dict(data))

    def clone(self):
        clone = AttrDict()
        clone.update_dict(self)
        return clone

    def rebase(self, base):
        base = base.clone()
        base.update_dict(self)
        self.clear()
        self.update_dict(base)

    def dump(self, stream=None, map_view=None):
        from ambry.metadata.proptree import _ScalarTermS, _ScalarTermU
        from ambry.orm import MutationList, MutationDict  # cross-module import

        yaml.representer.SafeRepresenter.add_representer(
            MapView, yaml.representer.SafeRepresenter.represent_dict)

        yaml.representer.SafeRepresenter.add_representer(
            AttrDict, yaml.representer.SafeRepresenter.represent_dict)

        yaml.representer.SafeRepresenter.add_representer(
            OrderedDict, yaml.representer.SafeRepresenter.represent_dict)

        yaml.representer.SafeRepresenter.add_representer(
            defaultdict, yaml.representer.SafeRepresenter.represent_dict)

        yaml.representer.SafeRepresenter.add_representer(
            MutationDict, yaml.representer.SafeRepresenter.represent_dict)

        yaml.representer.SafeRepresenter.add_representer(
            set, yaml.representer.SafeRepresenter.represent_list)

        yaml.representer.SafeRepresenter.add_representer(
            MutationList, yaml.representer.SafeRepresenter.represent_list)

        yaml.representer.SafeRepresenter.add_representer(
            IncludeFile, include_representer)

        yaml.representer.SafeRepresenter.add_representer(
            _ScalarTermS, yaml.representer.SafeRepresenter.represent_str)

        yaml.representer.SafeRepresenter.add_representer(
            _ScalarTermU, yaml.representer.SafeRepresenter.represent_str)

        if stream is None:
            stream = StringIO()

        d = self

        if map_view is not None:
            map_view.inner = d
            d = map_view

        try:
            yaml.safe_dump(d, stream, default_flow_style=False, indent=4, encoding='utf-8')
        except RepresenterError:
            pprint.pprint(self.to_dict())
            raise

        if isinstance(stream, StringIO):
            return stream.getvalue()

    def json(self):
        o = yaml.load(self.dump())
        return json.dumps(o)


class MapView(MutableMapping):
    """A map that provides a limited view on an underlying, inner map. Iterating over the
    view retrns only the keys specified in the keys argument. """

    _inner = None
    _keys = None

    def __init__(self, d=None, keys=None):
        self._inner = d
        self._keys = keys

    @property
    def inner(self):
        return self._inner

    @inner.setter
    def inner(self, value):
        self._inner = value

    def __getitem__(self, key):
        return self._inner.__getitem__(key)

    def __setitem__(self, key, value):
        raise NotImplementedError()
        return self._inner.__setitem__(key, value)

    def __delitem__(self, key):
        return self._inner.__delitem__(key)

    def __len__(self):
        return self._inner.__len__()

    def __iter__(self):

        for k in self._inner:
            if not self._keys or k in self._keys:
                yield k

    def __getattr__(self, item):
        return getattr(self._inner, item)


class CaseInsensitiveDict(Mapping):  # http://stackoverflow.com/a/16202162

    def __init__(self, d):
        self._d = d
        self._s = dict((k.lower(), k) for k in d)

    def __contains__(self, k):
        return k.lower() in self._s

    def __len__(self):
        return len(self._s)

    def __iter__(self):
        return iter(self._s)

    def __getitem__(self, k):
        return self._d[self._s[k.lower()]]

    def __setitem__(self, k, v):
        self._d[k] = v
        self._s[k.lower()] = k

    def pop(self, k):
        k0 = self._s.pop(k.lower())
        return self._d.pop(k0)

    def actual_key_case(self, k):
        return self._s.get(k.lower())


def lowercase_dict(d):
    return dict((k.lower(), v) for k, v in iteritems(d))


def configure_logging(cfg, custom_level=None):
    """Don't know what this is for ...."""
    import itertools as it
    import operator as op

    if custom_level is None:
        custom_level = logging.WARNING
    for entity in it.chain.from_iterable(it.imap(op.methodcaller('viewvalues'),
                                                 [cfg] + [cfg.get(k, dict()) for k in ['handlers', 'loggers']])):
        if isinstance(entity, Mapping) and entity.get('level') == 'custom':
            entity['level'] = custom_level
    logging.config.dictConfig(cfg)
    logging.captureWarnings(cfg.warnings)


# {{{ http://code.activestate.com/recipes/578272/ (r1)


def toposort(data):
    """Dependencies are expressed as a dictionary whose keys are items and
    whose values are a set of dependent items. Output is a list of sets in
    topological order. The first set consists of items with no dependences,
    each subsequent set consists of items that depend upon items in the
    preceeding sets.

>>> print '\\n'.join(repr(sorted(x)) for x in toposort2({
...     2: set([11]),
...     9: set([11,8]),
...     10: set([11,3]),
...     11: set([7,5]),
...     8: set([7,3]),
...     }) )
[3, 5, 7]
[8, 11]
[2, 9, 10]

    """

    # Ignore self dependencies.
    for k, v in iteritems(data):
        v.discard(k)
    # Find all items that don't depend on anything.
    extra_items_in_deps = reduce(
        set.union, itervalues(data)) - set(data.keys())
    # Add empty dependences where needed
    data.update({item: set() for item in extra_items_in_deps})
    while True:
        ordered = set(item for item, dep in iteritems(data) if not dep)
        if not ordered:
            break
        yield ordered
        data = {item: (dep - ordered)
                for item, dep in iteritems(data)
                if item not in ordered}

    assert not data, 'Cyclic dependencies exist among these items:\n%s' % '\n'.join(
        repr(x) for x in list(data.items()))


# end of http://code.activestate.com/recipes/578272/ }}}


def md5_for_stream(f, block_size=2 ** 20):

    md5 = hashlib.md5()

    while True:
        data = f.read(block_size)
        if not data:
            break

        md5.update(data)

        return md5.hexdigest()


def md5_for_file(f, block_size=2 ** 20):
    """Generate an MD5 has for a possibly large file by breaking it into
    chunks."""

    md5 = hashlib.md5()
    try:
        # Guess that f is a FLO.
        f.seek(0)

        return md5_for_stream(f, block_size=block_size)

    except AttributeError:
        # Nope, not a FLO. Maybe string?

        file_name = f
        with open(file_name, 'rb') as f:
            return md5_for_file(f, block_size)


def make_acro(past, prefix, s):  # pragma: no cover
    """Create a three letter acronym from the input string s.

    Args:
        past: A set object, for storing acronyms that have already been created
        prefix: A prefix added to the acronym before storing in the set
        s: The string to create the acronym from.

    """

    def _make_acro(s, t=0):
        """Make an acronym of s for trial t"""

        # Really should cache these ...
        v = ['a', 'e', 'i', 'o', 'u', 'y']
        c = [chr(x) for x in six_xrange(ord('a'), ord('z') + 1) if chr(x) not in v]

        s = re.sub(r'\W+', '', s.lower())

        vx = [x for x in s if x in v]  # Vowels in input string
        cx = [x for x in s if x in c]  # Consonants in input string

        if s.startswith('Mc'):

            if t < 1:
                return 'Mc' + v[0]
            if t < 2:
                return 'Mc' + c[0]

        if s[0] in v:  # Starts with a vowel
            if t < 1:
                return vx[0] + cx[0] + cx[1]
            if t < 2:
                return vx[0] + vx[1] + cx[0]

        if s[0] in c and s[1] in c:  # Two first consonants
            if t < 1:
                return cx[0] + cx[1] + vx[0]
            if t < 2:
                return cx[0] + cx[1] + cx[2]

        if t < 3:
            return cx[0] + vx[0] + cx[1]
        if t < 4:
            return cx[0] + cx[1] + cx[2]
        if t < 5:
            return cx[0] + vx[0] + vx[1]
        if t < 6:
            return cx[0] + cx[1] + cx[-1]

        # These are punts; just take a substring

        if t < 7:
            return s[0:3]
        if t < 8:
            return s[1:4]
        if t < 9:
            return s[2:5]
        if t < 10:
            return s[3:6]

        return None

    for t in six_xrange(11): # Try multiple forms until one isn't in the past acronyms

        try:
            a = _make_acro(s, t)

            if a is not None:
                if prefix:
                    aps = prefix + a
                else:
                    aps = a

                if aps not in past:
                    past.add(aps)
                    return a

        except IndexError:
            pass

    raise Exception('Could not get acronym.')


def ensure_dir_exists(path):
    """Given a file, ensure that the path to the file exists"""

    import os

    f_dir = os.path.dirname(path)

    if not os.path.exists(f_dir):
        os.makedirs(f_dir)

    return f_dir


def walk_dict(d):
    """Walk a tree (nested dicts).

    For each 'path', or dict, in the tree, returns a 3-tuple containing:
    (path, sub-dicts, values)

    where:
    * path is the path to the dict
    * sub-dicts is a tuple of (key,dict) pairs for each sub-dict in this dict
    * values is a tuple of (key,value) pairs for each (non-dict) item in this dict

    """
    # nested dict keys
    nested_keys = tuple(k for k in list(d.keys()) if isinstance(d[k], dict))
    # key/value pairs for non-dicts
    items = tuple((k, d[k]) for k in list(d.keys()) if k not in nested_keys)

    # return path, key/sub-dict pairs, and key/value pairs
    yield ('/', [(k, d[k]) for k in nested_keys], items)

    # recurse each subdict
    for k in nested_keys:
        for res in walk_dict(d[k]):
            # for each result, stick key in path and pass on
            res = ('/%s' % k + res[0], res[1], res[2])
            yield res


def init_log_rate(output_f, N=None, message='', print_rate=None):
    """Initialze the log_rate function. Returnas a partial function to call for
    each event.

    If N is not specified but print_rate is specified, the initial N is
    set to 100, and after the first message, the N value is adjusted to
    emit print_rate messages per second

    """

    if print_rate and not N:
        N = 100

    if not N:
        N = 5000

    d = [0,  # number of items processed
         time(),  # start time. This one gets replaced after first message
         N,  # ticker to next message
         N,  # frequency to log a message
         message,
         print_rate,
         deque([], maxlen=4)  # Deque for averaging last N rates
         ]

    assert isinstance(output_f, Callable)

    f = partial(_log_rate, output_f, d)
    f.always = output_f
    f.count = lambda: d[0]

    return f


def _log_rate(output_f, d, message=None):
    """Log a message for the Nth time the method is called.

    d is the object returned from init_log_rate

    """

    if d[2] <= 0:

        if message is None:
            message = d[4]

        # Average the rate over the length of the deque.
        d[6].append(int(d[3] / (time() - d[1])))
        rate = sum(d[6]) / len(d[6])

        # Prints the processing rate in 1,000 records per sec.
        output_f(message + ': ' + str(rate) + '/s ' + str(d[0] / 1000) + 'K ')

        d[1] = time()

        # If the print_rate was specified, adjust the number of records to
        # aproximate that rate.
        if d[5]:
            target_rate = rate * d[5]
            d[3] = int((target_rate + d[3]) / 2)

        d[2] = d[3]

    d[0] += 1
    d[2] -= 1


class Progressor(object):
    """Progress reporter suitable for calling in Library.get()

    Example:  r = l.get(args.term, cb=Progressor().progress)

    """

    start = None
    last = None
    freq = 5

    def __init__(self, message='Download', printf=print_):
        self.start = time.clock()
        self.message = message
        self.rates = deque(maxlen=10)
        self.printf = printf

    def progress(self, i, n):

        now = time.clock()

        if not self.last:
            self.last = now

        if now - self.last > self.freq:
            diff = now - self.start
            self.last = now

            i_rate = float(i) / diff
            self.rates.append(i_rate)

            if len(self.rates) > self.rates.maxlen / 2:
                rate = sum(self.rates) / len(self.rates)
                rate_type = 'a'
            else:
                rate = i_rate
                rate_type = 'i'

            msg = '{}: Compressed: {} Mb. Downloaded, Uncompressed: {:6.2f}  Mb, {:5.2f} Mb / s ({})'\
                .format(
                    self.message, int(int(n) / (1024 * 1024)),
                    round(float(i) / (1024. * 1024.), 2),
                    round(float(rate) / (1024 * 1024), 2), rate_type)
            self.printf(msg)


# http://stackoverflow.com/a/1695250
# >>> Numbers = enum('ZERO', 'ONE', TWO = 20, THREE = 30)
# >>> print Numbers.ONE
# >>> print Numbers.THREE


def enum(*sequential, **named):
    enums = dict(list(zip(sequential, list(six_xrange(len(sequential))))), **named)
    return type('Enum', (), enums)


class Constant:
    """Organizes constants in a class."""

    class ConstError(TypeError):
        pass

    def __setattr__(self, name, value):
        if name in self.__dict__:
            raise self.ConstError("Can't rebind const(%s)" % name)
        self.__dict__[name] = value


def count_open_fds():
    """return the number of open file descriptors for current process.

    .. warning: will only work on UNIX-like os-es.

    http://stackoverflow.com/a/7142094

    """

    pid = os.getpid()
    procs = subprocess.check_output(
        ['lsof', '-w', '-Ff', '-p', str(pid)])

    nprocs = len(
        [s for s in procs.split('\n') if s and s[0] == 'f' and s[1:].isdigit()]
    )
    return nprocs


def parse_url_to_dict(url):
    """Parse a url and return a dict with keys for all of the parts.

    The urlparse function() returns a wacky combination of a namedtuple
    with properties.

    """
    p = urlparse(url)

    return {
        'scheme': p.scheme,
        'netloc': p.netloc,
        'path': p.path,
        'params': p.params,
        'query': p.query,
        'fragment': p.fragment,
        'username': p.username,
        'password': p.password,
        'hostname': p.hostname,
        'port': p.port
    }


def unparse_url_dict(d):
    if 'hostname' in d and d['hostname']:
        host_port = d['hostname']
    else:
        host_port = ''

    if 'port' in d and d['port']:
        host_port += ':' + str(d['port'])

    user_pass = ''
    if 'username' in d and d['username']:
        user_pass += d['username']

    if 'password' in d and d['password']:
        user_pass += ':' + d['password']

    if user_pass:
        host_port = '{}@{}'.format(user_pass, host_port)

    url = '{}://{}/{}'.format(d.get('scheme', 'http'),
                              host_port, d.get('path', '').lstrip('/'))

    if 'query' in d and d['query']:
        url += '?' + d['query']

    return url


def set_url_part(url, **kwargs):
    """Change one or more parts of a URL"""
    d = parse_url_to_dict(url)

    d.update(kwargs)

    return unparse_url_dict(d)


def filter_url(url, **kwargs):
    """filter a URL by returning a URL with only the parts specified in the keywords"""

    d = parse_url_to_dict(url)

    d.update(kwargs)

    return unparse_url_dict({k: v for k, v in list(d.items()) if v})


def select_from_url(url, key):
    d = parse_url_to_dict(url)
    return d.get(key)


def normalize_newlines(string):
    """Convert \r\n or \r to \n."""
    return re.sub(r'(\r\n|\r|\n)', '\n', string)


def print_yaml(o):
    """Pretty print an object as YAML."""
    print(yaml.dump(o, default_flow_style=False, indent=4, encoding='utf-8'))


def qualified_class_name(o):
    """Full name of an object, including the module"""
    module = o.__class__.__module__
    if module is None or module == str.__class__.__module__:
        return o.__class__.__name__
    return module + '.' + o.__class__.__name__


def qualified_name(cls):
    """Full name of a class, including the module. Like qualified_class_name, but when you already have a class """
    module = cls.__module__
    if module is None or module == str.__class__.__module__:
        return cls.__name__
    return module + '.' + cls.__name__

def qualified_name_import(cls):
    """Full name of a class, including the module. Like qualified_class_name, but when you already have a class """

    parts = qualified_name(cls).split('.')

    return "from {} import {}".format('.'.join(parts[:-1]), parts[-1])



class _Getch:
    """Gets a single character from standard input.  Does not echo to the
screen."""
    def __init__(self):
        try:
            self.impl = _GetchWindows()
        except ImportError:
            self.impl = _GetchUnix()

    def __call__(self):
        return self.impl()


#from http://code.activestate.com/recipes/134892/
class _GetchUnix:
    def __init__(self):
        import tty, sys

    def __call__(self):
        import sys, tty, termios
        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        try:
            # Originally was raw mode, not cbreak, but raw screws up printing.
            tty.setcbreak(sys.stdin.fileno())
            ch = sys.stdin.read(1)
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
        return ch


class _GetchWindows:
    def __init__(self):
        import msvcrt

    def __call__(self):
        import msvcrt
        return msvcrt.getch()


getch = _Getch()


def scrape(library, url, as_html=False):

    if url.startswith('s3:'):
        s3 = library.filesystem.s3(url)
        return scrape_s3(url, s3, as_html=as_html)
    else:
        return scrape_urls_from_web_page(url)


def scrape_s3(root_url, s3, as_html=False):
    from os.path import join
    d = dict(external_documentation={}, sources={}, links={})

    for f in s3.walkfiles('/'):
        if as_html:
            try:
                url, _ = s3.getpathurl(f).split('?', 1)
            except ValueError:
                url = s3.getpathurl(f)
        else:
            url = join(root_url, f.strip('/'))

        fn = f.strip('/')

        d['sources'][fn] = dict(url=url, description='', title=fn)

    return d


def scrape_urls_from_web_page(page_url):
    parts = list(urlsplit(page_url))

    parts[2] = ''
    root_url = urlunsplit(parts)

    html_page = urlopen(page_url)
    soup = BeautifulSoup(html_page)

    d = dict(external_documentation={}, sources={}, links={})

    for link in soup.findAll('a'):

        if not link:
            continue

        if link.string:
            text = str(link.string.encode('ascii', 'ignore'))
        else:
            text = 'None'

        url = link.get('href')

        if not url:
            continue

        if 'javascript' in url:
            continue

        if url.startswith('http'):
            pass
        elif url.startswith('/'):
            url = os.path.join(root_url, url)
        else:
            url = os.path.join(page_url, url)

        base = os.path.basename(url)

        if '#' in base:
            continue

        try:
            fn, ext = base.split('.', 1)
        except ValueError:
            fn = base
            ext = ''

        try:  # Yaml adds a lot of junk to encode unicode. # FIXME. SHould use safe_dump instead
            fn = str(fn)
            url = str(url)
            text = str(text)
        except UnicodeDecodeError:
            pass

        # xlsm is a bug that adss 'm' to the end of the url. No idea.
        if ext.lower() in ('zip', 'csv', 'xls', 'xlsx', 'xlsm', 'txt'):
            d['sources'][fn] = dict(url=url, description=text)

        elif ext.lower() in ('pdf', 'html'):
            d['external_documentation'][fn] = dict(url=url, description=text, title=text)

        else:
            d['links'][text] = dict(url=url, description=text, title=text)

    return d


def drop_empty(rows):
    """Transpose the columns into rows, remove all of the rows that are empty after the first cell, then
    transpose back. The result is that columns that have a header but no data in the body are removed, assuming
    the header is the first row. """
    return zip(*[col for col in zip(*rows) if bool(filter(bool, col[1:]))])


# http://stackoverflow.com/a/20577580
def dequote(s):
    """
    If a string has single or double quotes around it, remove them.
    Make sure the pair of quotes match.
    If a matching pair of quotes is not found, return the string unchanged.
    """

    if (s[0] == s[-1]) and s.startswith(("'", '"')):
        return s[1:-1]

    return s


def pretty_time(s, granularity=3):
    """Pretty print time in seconds. COnverts the input time in seconds into a string with
    interval names, such as days, hours and minutes

    From:
    http://stackoverflow.com/a/24542445/1144479

    """

    intervals = (
        ('weeks', 604800),  # 60 * 60 * 24 * 7
        ('days', 86400),  # 60 * 60 * 24
        ('hours', 3600),  # 60 * 60
        ('minutes', 60),
        ('seconds', 1),
    )

    def display_time(seconds, granularity=granularity):
        result = []

        for name, count in intervals:
            value = seconds // count
            if value:
                seconds -= value * count
                if value == 1:
                    name = name.rstrip('s')
                result.append('{} {}'.format(int(value), name))

        return ', '.join(result[:granularity])

    return display_time(s, granularity)


# From: http://code.activestate.com/recipes/391367-deprecated/
def deprecated(func):
    """This is a decorator which can be used to mark functions
    as deprecated. It will result in a warning being emmitted
    when the function is used."""
    def newFunc(*args, **kwargs):
        warnings.warn("Call to deprecated function %s." % func.__name__,
                      category=DeprecationWarning)
        return func(*args, **kwargs)
    newFunc.__name__ = func.__name__
    newFunc.__doc__ = func.__doc__
    newFunc.__dict__.update(func.__dict__)
    return newFunc


def int_maybe(v):
    """Try to convert to an int and return None on failure"""
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def random_string(length):
    import random
    import string
    return ''.join(random.SystemRandom().choice(string.ascii_lowercase + string.ascii_uppercase + string.digits)
                   for _ in range(length))

# From: http://code.activestate.com/recipes/496741-object-proxying/

class Proxy(object):
    __slots__ = ["_obj", "__weakref__"]
    def __init__(self, obj):
        object.__setattr__(self, "_obj", obj)

    #
    # proxying (special cases)
    #
    def __getattribute__(self, name):
        return getattr(object.__getattribute__(self, "_obj"), name)
    def __delattr__(self, name):
        delattr(object.__getattribute__(self, "_obj"), name)
    def __setattr__(self, name, value):
        setattr(object.__getattribute__(self, "_obj"), name, value)

    def __nonzero__(self):
        return bool(object.__getattribute__(self, "_obj"))
    def __str__(self):
        return str(object.__getattribute__(self, "_obj"))
    def __repr__(self):
        return repr(object.__getattribute__(self, "_obj"))

    #
    # factories
    #
    _special_names = [
        '__abs__', '__add__', '__and__', '__call__', '__cmp__', '__coerce__',
        '__contains__', '__delitem__', '__delslice__', '__div__', '__divmod__',
        '__eq__', '__float__', '__floordiv__', '__ge__', '__getitem__',
        '__getslice__', '__gt__', '__hash__', '__hex__', '__iadd__', '__iand__',
        '__idiv__', '__idivmod__', '__ifloordiv__', '__ilshift__', '__imod__',
        '__imul__', '__int__', '__invert__', '__ior__', '__ipow__', '__irshift__',
        '__isub__', '__iter__', '__itruediv__', '__ixor__', '__le__', '__len__',
        '__long__', '__lshift__', '__lt__', '__mod__', '__mul__', '__ne__',
        '__neg__', '__oct__', '__or__', '__pos__', '__pow__', '__radd__',
        '__rand__', '__rdiv__', '__rdivmod__', '__reduce__', '__reduce_ex__',
        '__repr__', '__reversed__', '__rfloorfiv__', '__rlshift__', '__rmod__',
        '__rmul__', '__ror__', '__rpow__', '__rrshift__', '__rshift__', '__rsub__',
        '__rtruediv__', '__rxor__', '__setitem__', '__setslice__', '__sub__',
        '__truediv__', '__xor__', 'next',
    ]

    @classmethod
    def _create_class_proxy(cls, theclass):
        """creates a proxy for the given class"""

        def make_method(name):
            def method(self, *args, **kw):
                return getattr(object.__getattribute__(self, "_obj"), name)(*args, **kw)
            return method

        namespace = {}
        for name in cls._special_names:
            if hasattr(theclass, name):
                namespace[name] = make_method(name)
        return type("%s(%s)" % (cls.__name__, theclass.__name__), (cls,), namespace)

    def __new__(cls, obj, *args, **kwargs):
        """
        creates an proxy instance referencing `obj`. (obj, *args, **kwargs) are
        passed to this class' __init__, so deriving classes can define an
        __init__ method of their own.
        note: _class_proxy_cache is unique per deriving class (each deriving
        class must hold its own cache)
        """
        try:
            cache = cls.__dict__["_class_proxy_cache"]
        except KeyError:
            cls._class_proxy_cache = cache = {}
        try:
            theclass = cache[obj.__class__]
        except KeyError:
            cache[obj.__class__] = theclass = cls._create_class_proxy(obj.__class__)
        ins = object.__new__(theclass)
        theclass.__init__(ins, obj, *args, **kwargs)
        return ins




