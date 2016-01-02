"""Sqalchemy table for storing information about remote libraries

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from sqlalchemy import Column as SAColumn, Integer, UniqueConstraint
from sqlalchemy import Text, String, ForeignKey, LargeBinary
from sqlalchemy import event
from simplecrypt import encrypt, decrypt, DecryptionException as SC_DecryptionException
import simplecrypt
from ambry.orm.exc import OrmObjectError
import os.path


from . import Base, MutationDict, JSONEncodedObj

class Remote(Base):

    __tablename__ = 'remote'

    id = SAColumn('rm_id', Integer, primary_key=True)

    short_name = SAColumn('rm_short_name', Text, index=True, unique=True)

    service = SAColumn('rm_service', Text, index=True) # ambry, s3 or fs

    url = SAColumn('rm_url', Text)

    d_vid = SAColumn('rm_d_vid', String(20), ForeignKey('datasets.d_vid'),  index=True)

    docker_url = SAColumn('rm_docker_url', Text)
    docker_tls_cert_path = SAColumn('rm_docker_cert_path', Text)
    docker_tls_ca_cert = SAColumn('rm_docker_ca_cert', Text)
    docker_tls_cert = SAColumn('rm_docker_cert', Text)
    docker_tls_key = SAColumn('rm_docker_key', Text)

    db_name = SAColumn('rm_db_name', Text)
    vol_name = SAColumn('rm_vol_name', Text)
    ui_name = SAColumn('rm_ui_name', Text)

    db_dsn = SAColumn('rm_db_dsn', Text)
    jwt_secret = SAColumn('rm_api_token', Text, doc='Encryption secret for JWT')

    account_password = SAColumn('rm_account_password', Text, doc='Password for encryption secrets in the database')

    comment = SAColumn('ac_comment', Text)  # Access token or username
    message = SAColumn('rm_message', Text)

    data = SAColumn('rm_data', MutationDict.as_mutable(JSONEncodedObj))

    # Temp variables, not stored

    account_accessor = None # Set externally to allow access to the account credentials

    tr_db_password = None

    @property
    def api_token(self): # old name
        return self.jwt_secret

    @property
    def is_api(self):
        return self.service in ('ambry','docker')

    @property
    def dict(self):
        """A dict that holds key/values for all of the properties in the
        object.

        :return:

        """
        from collections import OrderedDict

        d = OrderedDict([ (p.key,getattr(self, p.key)) for p in self.__mapper__.attrs if p.key not in ('data') ])

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
        from ambry.util import parse_url_to_dict, set_url_part

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

    def list(self):
        """List all of the bundles in the remote"""

        if self.is_api:
            return self._list_api()
        else:
            return self._list_fs()


    def _list_fs(self):
        assert self.account_accessor

        remote = self.s3(self.url, self.account_accessor)

        return remote.listdir('_meta/vname')

    def _list_api(self):

        c = self._api_client()

        return [ d.name for d in c.list()]


    def find(self, ref):

        if self.is_api:
            return self._find_api(ref)
        else:
            return self._find_fs(ref)


    def _find_fs(self, ref):
        from fs.errors import ResourceNotFoundError
        from ambry.orm.exc import NotFoundError
        import json

        remote = self.s3(self.url, self.account_accessor)

        path_parts = ['vid','id','vname','name']

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

    def checkin(self, package,  cb=None):
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
            return self._checkin_api(package, cb)
        else:
            return self._checkin_fs(package, cb)

    def _checkin_fs(self, package, cb=None):

        assert self.account_accessor

        remote = self.s3(self.url, self.account_accessor)

        ds = package.package_dataset

        db_ck = ds.identity.cache_key + '.db'

        if cb:
            def cb_one_arg(n):
                cb('Uploading package', n)
        else:
            cb_one_arg = None

        with open(package.path) as f:
            remote.makedir(os.path.dirname(db_ck), recursive=True, allow_recreate=True)
            e = remote.setcontents_async(db_ck, f, progress_callback=cb_one_arg)
            e.wait()

        if package.library:
            from ambry.orm import Bundle
            bundle = Bundle(ds, package.library)

            for p in bundle.partitions:
                self._put_partition_fs(remote, p, cb=cb)

        self._put_metadata(remote, ds)

        return remote, remote.getpathurl(db_ck)

    def _checkin_api(self, package, cb=None):
        from ambry_client import Client

        c = self._api_client()

        return c.library.checkin(package, cb)


    def _put_metadata(self, fs_remote, ds):
        """Store metadata on a pyfs remote"""
        import json
        from six import text_type

        identity = ds.identity
        d = identity.dict

        d['summary'] = ds.config.metadata.about.summary
        d['title'] = ds.config.metadata.about.title

        ident = json.dumps(d)

        fs_remote.setcontents(os.path.join('_meta', 'vid', identity.vid), ident)
        fs_remote.setcontents(os.path.join('_meta', 'id', identity.id_), ident)
        fs_remote.setcontents(os.path.join('_meta', 'vname', text_type(identity.vname)), ident)
        fs_remote.setcontents(os.path.join('_meta', 'name', text_type(identity.name)), ident)



    def put_partition(self, cb=None):
        """Store a partition on the remote"""
        pass


    def _put_partition_fs(self, fs_remote, p, cb=None):

        if cb:
            def cb_one_arg(n):
                cb('Uploading partition {}'.format(p.identity.name), n)
        else:
            cb_one_arg = None

        with p.datafile.open(mode='rb') as fin:
            fs_remote.makedir(os.path.dirname(p.datafile.path), recursive=True, allow_recreate=True)
            event = fs_remote.setcontents_async(p.datafile.path, fin, progress_callback=cb_one_arg)
            event.wait()

    def _put_partition_api(self, p, cb=None):
        pass


    def checkout(self):
        """Checkout a bundle from the remote"""
        pass

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

        remote = self.s3(self.url, self.account_accessor)

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

        return info['vid']

    def _remove_api(self, ref, cb=None):
        from ambry_client import Client

        info = self._find_api(ref)

        c = self._api_client()

        c.library.remove(ref)

    def s3(self, url, account_acessor):
        """Setup an S3 pyfs, with account credentials, fixing an ssl matching problem"""
        from fs.s3fs import S3FS
        from ambry.util import parse_url_to_dict
        from ambry.dbexceptions import ConfigurationError
        import ssl

        assert self.account_accessor

        _old_match_hostname = ssl.match_hostname

        def _new_match_hostname(cert, hostname):
            if hostname.endswith('.s3.amazonaws.com'):
                pos = hostname.find('.s3.amazonaws.com')
                hostname = hostname[:pos].replace('.', '') + hostname[pos:]
            return _old_match_hostname(cert, hostname)

        ssl.match_hostname = _new_match_hostname

        pd = parse_url_to_dict(url)

        account = account_acessor(pd['hostname'])

        assert account['account_id'] == pd['hostname']

        s3 = S3FS(
            bucket=pd['netloc'],
            prefix=pd['path'],
            aws_access_key=account['access_key'],
            aws_secret_key=account['secret'],

        )

        # ssl.match_hostname = _old_match_hostname

        return s3

    def __str__(self):
        return '{};{}'.format(self.short_name,self.url)

    @staticmethod
    def before_insert(mapper, conn, target):
        Remote.before_update(mapper, conn, target)

    @staticmethod
    def before_update(mapper, conn, target):

        url = target.url

        if not target.service:
            if url.startswith('s3:'):
                target.service = 's3'
            elif url.startswith('http'):
                target.service = 'ambry'
            else:
                target.service = 'fs'



event.listen(Remote, 'before_insert', Remote.before_insert)
event.listen(Remote, 'before_update', Remote.before_update)