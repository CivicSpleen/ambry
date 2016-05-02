"""Sqalchemy table for storing information about remote libraries

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from sqlalchemy import Column as SAColumn, Integer
from sqlalchemy import Text, String, ForeignKey
from sqlalchemy import event
import os.path
import ssl

from . import Base, MutationDict, JSONEncodedObj

import logging
from ambry.util import get_logger
logger = get_logger(__name__)
#logger.setLevel(logging.DEBUG)

from fs.opener import Opener, opener

class HTTPSOpener(Opener):
    names = ['https']
    desc = """HTTPS file opener. HTTPS only supports reading files, and not much else.

example:
* https://www.example.org/index.html"""

    @classmethod
    def get_fs(cls, registry, fs_name, fs_name_params, fs_path, writeable, create_dir):
        from fs.httpfs import HTTPFS
        if '/' in fs_path:
            dirname, resourcename = fs_path.rsplit('/', 1)
        else:
            dirname = fs_path
            resourcename = ''
        fs = HTTPFS('https://' + dirname)
        return fs, resourcename

opener.add(HTTPSOpener)

def patch_match_hostname():
    """Fixes https://github.com/boto/boto/issues/2836"""


    _old_match_hostname = ssl.match_hostname

    def _new_match_hostname(cert, hostname):
        if hostname.endswith('.s3.amazonaws.com'):
            pos = hostname.find('.s3.amazonaws.com')
            hostname = hostname[:pos].replace('.', '') + hostname[pos:]
        return _old_match_hostname(cert, hostname)

    ssl.match_hostname = _new_match_hostname

patch_match_hostname()


class RemoteAccessError(Exception):

    """Failed to access a remote"""


class Remote(Base):

    __tablename__ = 'remote'

    id = SAColumn('rm_id', Integer, primary_key=True)

    short_name = SAColumn('rm_short_name', Text, index=True, unique=True)

    service = SAColumn('rm_service', Text, index=True)  # ambry, s3 or fs

    url = SAColumn('rm_url', Text)

    d_vid = SAColumn('rm_d_vid', String(20), ForeignKey('datasets.d_vid'),  index=True)

    access = SAColumn('rm_access', Text, doc='Access key or username')
    secret = SAColumn('rm_secret', Text, doc='Secret key or password')

    # These are deprecated. They are properties of a host, not a remote
    docker_url = SAColumn('rm_docker_url', Text)

    # These are deprecated, and should be removed when docker support is changed
    db_name = SAColumn('rm_db_name', Text)
    vol_name = SAColumn('rm_vol_name', Text)
    db_dsn = SAColumn('rm_db_dsn', Text)

    # Base virtual host name, applied to the docker host.
    virtual_host = SAColumn('rm_virtual_host', Text, doc='Virtual host name, for web proxy')

    data = SAColumn('rm_data', MutationDict.as_mutable(JSONEncodedObj))

    # Temp variables, not stored

    account_accessor = None  # Set externally to allow access to the account credentials

    tr_db_password = None

    @property
    def api_token(self):  # old name
        return self.jwt_secret

    @property
    def is_api(self):
        return self.service in ('ambry', 'docker')

    @property
    def dict(self):
        """A dict that holds key/values for all of the properties in the
        object.

        :return:

        """
        from collections import OrderedDict

        d = OrderedDict(
            [(p.key, getattr(self, p.key)) for p in self.__mapper__.attrs if p.key not in ('data',)])

        if 'list' in self.data:
            d['bundle_count'] = len(self.data['list'])
        else:
            d['bundle_count'] = None

        if self.data:
            for k, v in self.data.items():
                d[k] = v

        return d

    @property
    def db_password(self):
        from ambry.util import parse_url_to_dict

        d = parse_url_to_dict(self.db_dsn)

        return d['password']

    @property
    def db_host(self):
        from ambry.util import parse_url_to_dict

        d = parse_url_to_dict(self.db_dsn)

        return d['hostname']

    def _api_client(self):
        from ambry_client import Client
        from ambry.util import set_url_part

        username = 'api'

        try:
            account = self.account_accessor(set_url_part(self.url, username=username))

        except KeyError:
            pass

        c = Client(self.url, username, account['secret'])
        return c

    @property
    def api_client(self):
        return self._api_client()

    def update(self):
        """Cache the list into the data section of the record"""

        from ambry.orm.exc import NotFoundError
        from requests.exceptions import ConnectionError, HTTPError
        from boto.exception import S3ResponseError

        d = {}

        try:
            for k, v in self.list(full=True):
                if not v:
                    continue

                d[v['vid']] = {
                    'vid': v['vid'],
                    'vname': v.get('vname'),
                    'id': v.get('id'),
                    'name': v.get('name')
                }

            self.data['list'] = d
        except (NotFoundError, ConnectionError, S3ResponseError, HTTPError) as e:
            raise RemoteAccessError("Failed to update {}: {}".format(self.short_name, e))

    def list(self, full=False):
        """List all of the bundles in the remote"""

        if self.is_api:
            return self._list_api(full=full)
        else:
            return self._list_fs(full=full)

    def _list_fs(self, full=False):
        assert self.account_accessor
        from fs.errors import ResourceNotFoundError
        from os.path import join
        from json import loads

        remote = self._fs_remote(self.url)

        # HTTP can't list, so we have to use a cached collection of list entries.
        # Use 'ambry remote <remote> update-listing' to create the cache

        if self.url.startswith('http'):
            try:
                for e in  loads(remote.getcontents(os.path.join('_meta', 'list.json'))):
                    if full:
                        yield (e['vname'], e)
                    else:
                        yield e['vname']


            except ResourceNotFoundError:
                return

        try:
            for e in remote.listdir('_meta/vname'):

                if full:
                    r = loads(remote.getcontents(join('_meta/vname', e)))
                    yield (e, r)
                else:
                    yield e
        except ResourceNotFoundError:
            # An old repo, doesn't have the meta/name values.
            for fn in remote.walkfiles(wildcard='*.db'):
                this_name = fn.strip('/').replace('/', '.').replace('.db', '')
                if full:
                    yield this_name
                else:
                    # Isn't any support for this
                    yield (this_name, None)

    def _update_fs_list(self):
        """Cache the full list for http access. This creates a meta file that can be read all at once,
        rather than requiring a list operation like S3 access does"""
        from json import dumps

        full_list = [ e[1] for e in self._list_fs(full=True) ]

        remote = self._fs_remote(self.url)

        remote.setcontents(os.path.join('_meta', 'list.json'), dumps(full_list, indent = 4))


    def _list_api(self, full=False):

        c = self._api_client()

        for d in c.list():
            if full:
                yield (d.name, d)
            else:
                yield d.name

    def find(self, ref):

        if self.is_api:
            return self._find_api(ref)
        else:
            return self._find_fs(ref)

    def _find_fs(self, ref):
        from fs.errors import ResourceNotFoundError
        from ambry.orm.exc import NotFoundError
        import json

        remote = self._fs_remote(self.url)

        path_parts = ['vname', 'vid', 'name', 'id']

        for p in path_parts:
            path = "/_meta/{}/{}".format(p, ref)

            try:
                e = remote.getcontents(path)
                return json.loads(e)
            except ResourceNotFoundError:
                pass

        raise NotFoundError("Failed to find bundle for ref '{}' ".format(ref))

    def _find_api(self, ref):
        c = self._api_client()

        return c.dataset(ref)

    def checkin(self, package, no_partitions=False, force=False,  cb=None):
        """
        Check in a bundle package to the remote.

        :param package: A Database, referencing a sqlite database holding the bundle
        :param cb: a two argument progress callback: cb(message, num_records)
        :return:
        """
        from ambry.orm.exc import NotFoundError

        if not os.path.exists(package.path):
            raise NotFoundError("Package path does not exist: '{}' ".format(package.path))

        if self.is_api:
            return self._checkin_api(package, no_partitions=no_partitions, force=force, cb=cb)
        else:
            return self._checkin_fs(package, no_partitions=no_partitions, force=force, cb=cb)

    def _checkin_fs(self, package, no_partitions=False, force = False,  cb=None):
        from fs.errors import NoPathURLError, NoSysPathError
        from ambry.orm import Partition
        assert self.account_accessor

        remote = self._fs_remote(self.url)

        ds = package.package_dataset

        db_ck = ds.identity.cache_key + '.db'

        if cb:
            def cb_one_arg(n):
                cb('Uploading package', n)
        else:
            def cb_one_arg(n):
                logger.info('Uploading package {} bytes'.format(n))

        with open(package.path) as f:
            remote.makedir(os.path.dirname(db_ck), recursive=True, allow_recreate=True)
            e = remote.setcontents_async(db_ck, f, progress_callback=cb_one_arg)
            e.wait()


        if package.library:
            for p in package.session.query(Partition).filter(Partition.type == Partition.TYPE.UNION).all():

                self._put_partition_fs(remote, p, package.library, force = force, cb=cb)

        self._put_metadata(remote, ds)


        try:
            return remote, remote.getpathurl(db_ck)
        except NoPathURLError:
            pass

        try:
            return remote, remote.getsyspath(db_ck)
        except NoSysPathError:
            pass


        return remote, None

    def _checkin_api(self, package, no_partitions=False, force = False, cb=None):

        c = self._api_client()

        return c.library.checkin(package, force=force, cb=cb)

    def _put_metadata(self, fs_remote, ds):
        """Store metadata on a pyfs remote"""
        import json
        from six import text_type
        from fs.errors import ResourceNotFoundError

        identity = ds.identity
        d = identity.dict

        d['summary'] = ds.config.metadata.about.summary
        d['title'] = ds.config.metadata.about.title

        ident = json.dumps(d)

        def do_metadata():
            fs_remote.setcontents(os.path.join('_meta', 'vid', identity.vid), ident)
            fs_remote.setcontents(os.path.join('_meta', 'id', identity.id_), ident)
            fs_remote.setcontents(os.path.join('_meta', 'vname', text_type(identity.vname)), ident)
            fs_remote.setcontents(os.path.join('_meta', 'name', text_type(identity.name)), ident)

        try:
            do_metadata()
        except ResourceNotFoundError:
            parts = ['vid', 'id', 'vname', 'name']
            for p in parts:
                dirname = os.path.join('_meta', p)
                fs_remote.makedir(dirname, allow_recreate=True, recursive=True)

            do_metadata()

    def put_partition(self, cb=None):
        """Store a partition on the remote"""
        raise NotImplementedError()
        pass

    def _put_partition_fs(self, fs_remote, p, library,  force = False, cb=None):

        if cb:
            def cb_one_arg(n):
                cb('Uploading partition {}'.format(p.identity.name), n)
        else:
            cb_one_arg = None

        if not library:
            return

        p = library.partition(p.vid)

        with p.datafile.open(mode='rb') as fin:
            fs_remote.makedir(os.path.dirname(p.datafile.path), recursive=True, allow_recreate=True)

            exists = fs_remote.exists(p.datafile.path)

            if force or not exists:
                event = fs_remote.setcontents_async(p.datafile.path, fin, progress_callback=cb_one_arg)
                event.wait()
            else:
                cb('Partition {} already exists on remote'.format(p.vid), 0)

    def _put_partition_api(self, p, cb=None):
        raise NotImplementedError()
        pass

    def checkout(self, ref, cb=None):
        """Checkout a bundle from the remote. Returns a file-like object"""
        if self.is_api:
            return self._checkout_api(ref, cb=cb)
        else:
            return self._checkout_fs(ref, cb=cb)

    def _checkout_api(self, ref,  cb=None):
        raise NotImplementedError()

    def _checkout_fs(self, ref, cb=None):
        remote = self._fs_remote(self.url)

        d = self._find_fs(ref)

        return remote.open(d['cache_key'] + '.db', 'rb')

    def get_partition(self):
        """Get a partition from the remote"""
        pass

    def remove(self, ref, cb=None):
        """Check in a bundle to the remote"""

        if self.is_api:
            return self._remove_api(ref, cb)
        else:
            return self._remove_fs(ref, cb)

    def _remove_fs(self, ref, cb=None):
        from fs.errors import ResourceNotFoundError
        from os.path import join

        remote = self._fs_remote(self.url)

        def safe_remove(path):
            try:
                remote.remove(path)
                if cb:
                    cb('Removed {}'.format(path))
            except ResourceNotFoundError as e:
                if cb:
                    cb("Failed to remove '{}': {}".format(path, e))

        info = self._find_fs(ref)

        db_ck = info['cache_key'] + '.db'

        if cb:
            cb('Removing {}'.format(db_ck))

            safe_remove(db_ck)

        for dir, files in remote.walk(info['cache_key']):
            for f in files:
                path = join(dir, f)

                safe_remove(path)

        for p in [join('_meta', 'vid', info['vid']), join('_meta', 'id', info['id']),
                  join('_meta', 'vname', info['vname']), join('_meta', 'name', info['name'])]:
            safe_remove(p)

        # FIXME! Doesn't remove partitions

        return info['vid']

    def _remove_api(self, ref, cb=None):

        info = self._find_api(ref)

        c = self._api_client()

        c.library.remove(ref)

    def _fs_remote(self, url):

        from ambry.util import parse_url_to_dict

        d = parse_url_to_dict(url)

        if d['scheme'] == 's3':
            return self.s3(url, access=self.access, secret=self.secret)
        else:
            from fs.opener import fsopendir
            return fsopendir(url)

    @property
    def fs(self):
        """Return a pyfs object"""
        return self._fs_remote(self.url)

    def s3(self, url, account_acessor=None, access=None, secret=None):
        """Setup an S3 pyfs, with account credentials, fixing an ssl matching problem"""
        from fs.s3fs import S3FS
        from ambry.util import parse_url_to_dict
        import ssl


        pd = parse_url_to_dict(url)

        if account_acessor:
            account = account_acessor(pd['hostname'])

            assert account['account_id'] == pd['hostname']
            aws_access_key = account['access_key'],
            aws_secret_key = account['secret']
        else:
            aws_access_key = access
            aws_secret_key = secret

        assert access, url
        assert secret, url


        s3 = S3FS(
            bucket=pd['netloc'],
            prefix=pd['path'],
            aws_access_key=aws_access_key,
            aws_secret_key=aws_secret_key,

        )

        return s3

    def __str__(self):
        return '{};{}'.format(self.short_name, self.url)

    @staticmethod
    def before_insert(mapper, conn, target):
        Remote.before_update(mapper, conn, target)

    @staticmethod
    def before_update(mapper, conn, target):

        url = target.url

        if not target.service and url:
            if url.startswith('s3:'):
                target.service = 's3'
            elif url.startswith('http'):
                target.service = 'ambry'
            else:
                target.service = 'fs'

event.listen(Remote, 'before_insert', Remote.before_insert)
event.listen(Remote, 'before_update', Remote.before_update)
