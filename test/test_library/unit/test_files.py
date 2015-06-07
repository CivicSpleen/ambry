'''
Created on Jun 30, 2012

@author: eric
'''
import hashlib
import os
from StringIO import StringIO
from tempfile import NamedTemporaryFile
import unittest

from sqlalchemy.orm.exc import NoResultFound

import fudge

from ambry.library.database import LibraryDb
from ambry.library.files import Files
from ambry.dbexceptions import ObjectStateError, NotFoundError
from ambry.orm import File
from test.test_library.factories import FileFactory, PartitionFactory


class FilesTest(unittest.TestCase):
    def setUp(self):
        self.sqlite_db = LibraryDb(driver='sqlite', dbname='test_database.db')
        self.sqlite_db.enable_delete = True
        self.sqlite_db.create_tables()
        FileFactory._meta.sqlalchemy_session = self.sqlite_db.session
        PartitionFactory._meta.sqlalchemy_session = self.sqlite_db.session

    def tearDown(self):
        try:
            os.remove('test_database.db')
        except OSError:
            pass

        fudge.clear_calls()
        fudge.clear_expectations()

    # helpers
    def _assert_filtered_by(self, file_field, values, files_method=None, filter_by=None):
        """ checks for filtering.
        Args:
            file_field (str): which field of the File to populate with values
            values (list): values for creation. New File instance will be created for each value.
            files_method (str, optional): which method of the Files to call to filter.
                By default equals to file_field.
            filter_by (str, optional): what value to use to filter. If none, first
                values from values will be used.
        Raises:
            AssertionError on wrong filter.
        """
        # create file for each value
        if files_method is None:
            files_method = file_field
        if filter_by is None:
            filter_by = values[0]
        assert len(values) > 1
        for value in values:
            FileFactory(**{file_field: value})
        self.sqlite_db.session.commit()

        # create Files instance with all files
        query = self.sqlite_db.session.query(File)
        all_files = Files(self.sqlite_db, query=query)

        # now filter and test. New instance should have only one file with given state.
        filtered = getattr(all_files, files_method)(filter_by)
        self.assertEquals(len(filtered.all), 1)
        file1 = filtered.all[0]
        self.assertEquals(getattr(file1, file_field), filter_by)

    def test_all_property_returns_all_records(self):
        # TODO: return the same value as SQLAlchemy returns
        all_result = [['elem1'], ['elem2']]
        query = fudge.Fake()\
            .expects('all')\
            .returns(all_result)
        db = None  # does not matter right now
        f1 = Files(db, query)
        self.assertEquals(f1.all, all_result)

    def test_first_property_returns_first_record(self):
        # TODO: return the same value as SQLAlchemy returns
        first_result = ['elem1']
        query = fudge.Fake()\
            .expects('first')\
            .returns(first_result)
        db = None  # does not matter right now
        f1 = Files(db, query)
        self.assertEquals(f1.first, first_result)

    def test_one_property_returns_single_record(self):
        # TODO: return the same value as SQLAlchemy returns
        one_result = ['elem1']
        query = fudge.Fake()\
            .expects('one')\
            .returns(one_result)
        db = None  # does not matter right now
        f1 = Files(db, query)
        self.assertEquals(f1.one, one_result)

    def test_one_maybe_returns_single_record(self):
        # TODO: return the same value as SQLAlchemy returns
        one_result = ['elem1']
        query = fudge.Fake()\
            .expects('one')\
            .returns(one_result)
        db = None  # does not matter right now
        f1 = Files(db, query)
        self.assertEquals(f1.one_maybe, one_result)

    def test_one_maybe_returns_none_if_there_is_no_record(self):
        # TODO: return the same value as SQLAlchemy returns
        query = fudge.Fake()\
            .expects('one')\
            .raises(NoResultFound)
        db = None  # does not matter right now
        f1 = Files(db, query)
        self.assertEquals(f1.one_maybe, None)

    def test_order_returns_instance_with_ordered_query(self):
        order_column = 1
        query = fudge.Fake()\
            .expects('order_by')\
            .with_args(order_column)
        db = None  # does not matter right now
        f1 = Files(db, query)
        self.assertEquals(f1.order(order_column), f1)

    def test_delete_deletes_all_records(self):
        query = fudge.Fake()\
            .expects('delete')\
            .expects('count')\
            .returns(1)
        db = fudge.Fake()\
            .expects('commit')
        f1 = Files(db, query)
        self.assertIsNone(f1.delete())

    def test_update_updates_records_in_the_query(self):
        # TODO: change record format to match to the real value.
        records = ['1']
        query = fudge.Fake()\
            .expects('update')\
            .with_args(records)
        db = None
        f1 = Files(db, query)
        self.assertIsNone(f1.update(records))

    # .query tests
    # TODO: Implement.
    # TODO: Do we really need .query property? Check for usage.

    # .ref tests
    def test_returns_files_instance_with_query_filtered_by_given_ref(self):
        # create many files
        self._assert_filtered_by('ref', ['ref1', 'ref2'], filter_by='ref1')

    # .state tests
    def test_returns_files_instance_with_query_filtered_by_given_state(self):
        # create many files
        self._assert_filtered_by('state', ['state1', 'state2'], filter_by='state1')

    # .path tests
    def test_returns_files_instance_with_query_filtered_by_given_path(self):
        self._assert_filtered_by('path', ['/path1', '/path2'], filter_by='/path1')

    # .type tests
    def test_returns_files_instance_with_query_filtered_by_given_type(self):
        self._assert_filtered_by(
            'type_', ['type1', 'type2'],
            files_method='type', filter_by='type1')

    # .source_url tests
    def test_returns_files_instance_with_query_filtered_by_given_source_url(self):
        self._assert_filtered_by(
            'source_url',
            ['http://example.com/1', 'http://example.com/2'],
            filter_by='http://example.com/1')

    # .installed tests
    def test_returns_files_instance_with_query_with_installed_files(self):
        # create file for each value
        f1 = FileFactory(type_=Files.TYPE.BUNDLE)
        f2 = FileFactory(type_=Files.TYPE.PARTITION)
        f3 = FileFactory(type_=Files.TYPE.SOURCE)
        self.sqlite_db.session.commit()

        # create Files instance with all files
        query = self.sqlite_db.session.query(File)
        all_files = Files(self.sqlite_db, query=query)

        installed = all_files.installed
        self.assertEquals(len(installed.all), 2)
        self.assertIn(f1, installed.all)
        self.assertIn(f2, installed.all)
        self.assertNotIn(f3, installed.all)

    # .new_file tests
    def test_returns_new_file(self):
        query = self.sqlite_db.session.query(File)
        all_files = Files(self.sqlite_db, query=query)
        self.assertEquals(len(all_files.all), 0)
        new_f = all_files.new_file(
            oid='oid1', path='/path1',
            source_url='http://example.com', type_=Files.TYPE.BUNDLE)
        self.assertIsInstance(new_f, File)
        self.assertEquals(new_f.oid, 1)
        self.assertEquals(new_f.path, '/path1')
        self.assertEquals(new_f.type_, Files.TYPE.BUNDLE)
        self.assertEquals(new_f.source_url, 'http://example.com')

    # .merge tests
    def test_saves_given_file_to_db(self):
        query = self.sqlite_db.session.query(File)
        all_files = Files(self.sqlite_db, query=query)
        f1 = File(
            oid=1, path='/path1',
            type=File.TYPE.PARTITION, source_url='http://example.com')
        all_files.merge(f1)
        self.sqlite_db.session.commit()
        self.assertEquals(len(all_files.all), 1)
        self.assertEquals(f1.oid, all_files.all[0].oid)

    # .install_bundle tests
    def test_saves_bundle_to_db(self):

        # TODO: Create BundleFactory and use it here.
        class FakeBundle(object):
            database = fudge.Fake().has_attr(path='/path1')
            identity = fudge.Fake().has_attr(vid='1')

        bundle = FakeBundle()
        source = 'http://example.com'

        query = self.sqlite_db.session.query(File)
        all_files = Files(self.sqlite_db, query=query)
        new_f = all_files.install_bundle_file(bundle, source)

        installed = all_files.all[0]
        self.assertEquals(installed.path, bundle.database.path)
        self.assertEquals(installed.ref, bundle.identity.vid)
        self.assertEquals(installed.type_, Files.TYPE.BUNDLE)
        self.assertEquals(installed.data, {})
        self.assertEquals(installed.source_url, source)

        # the same file returned
        self.assertEquals(new_f.path, installed.path)
        self.assertEquals(new_f.ref, installed.ref)
        self.assertEquals(new_f.type_, installed.type_)

    # .install_partition tests
    # TODO: do not use PartitionFactory here. It requres ambry.partition.BasePartition
    # subclass (SqlitePartition for ex.).

    # .install_bundle_source tests
    def test_saves_bundle_source_to_db(self):
        identity = fudge.Fake('identity').has_attr(vid='1')
        bundle = fudge.Fake('bundle').has_attr(
            bundle_dir='/tmp', identity=identity,
            build_state='1')
        source = 'http://example.com'

        query = self.sqlite_db.session.query(File)
        all_files = Files(self.sqlite_db, query=query)
        new_f = all_files.install_bundle_source(bundle, source)

        installed = all_files.all[0]
        self.assertEquals(installed.path, bundle.bundle_dir)
        self.assertEquals(installed.ref, bundle.identity.vid)
        self.assertEquals(installed.type_, Files.TYPE.SOURCE)
        self.assertEquals(installed.data, {})
        self.assertEquals(installed.hash, None)
        self.assertEquals(installed.priority, None)

        # the same file returned
        self.assertEquals(new_f.path, installed.path)
        self.assertEquals(new_f.ref, installed.ref)
        self.assertEquals(new_f.type_, installed.type_)

    # .install_data_store tests
    def test_saves_data_store_to_db(self):
        # TODO: Create WarehouseFactory and use it here
        warehouse = fudge.Fake('warehouse')\
            .has_attr(
                dsn='dsn', uid='uid', database_class='db_class')
        name = 'name'
        title = 'title'
        summary = 'summary'
        cache = 'cache'
        url = 'http://example.com'

        query = self.sqlite_db.session.query(File)
        all_files = Files(self.sqlite_db, query=query)
        new_f = all_files.install_data_store(
            warehouse, name=name, title=title,
            summary=summary, cache=cache, url=url)

        installed = all_files.all[0]
        self.assertEquals(installed.path, warehouse.dsn)
        self.assertEquals(installed.ref, warehouse.uid)
        self.assertEquals(installed.type_, Files.TYPE.STORE)
        self.assertIn('name', installed.data)
        self.assertEquals(installed.data['name'], name)
        self.assertIn('title', installed.data)
        self.assertEquals(installed.data['title'], title)
        self.assertIn('summary', installed.data)
        self.assertEquals(installed.data['summary'], summary)
        self.assertIn('cache', installed.data)
        self.assertEquals(installed.data['cache'], cache)
        self.assertIn('url', installed.data)
        self.assertEquals(installed.data['url'], url)

        # the same file was returned.
        self.assertEquals(new_f.path, installed.path)
        self.assertEquals(new_f.ref, installed.ref)
        self.assertEquals(new_f.type_, installed.type_)

    # .install_manifest tests
    @fudge.patch('requests.get')
    def test_saves_manifest_to_db(self, fake_get):
        url_content = 'url_elem1,url_elem2'

        class FakeResponse(object):
            content = url_content

            def raise_for_status(*args, **kwargs):
                pass

        fake_get.expects_call()\
            .returns(FakeResponse())

        # TODO: Create ManifestFactory and use it here.
        class FakeManifest(object):
            path = 'http://example.com'
            uid = 'http://example.com'
            dict = {'a': 'b'}

        manifest = FakeManifest()
        query = self.sqlite_db.session.query(File)
        all_files = Files(self.sqlite_db, query=query)

        new_f = all_files.install_manifest(manifest)

        installed = all_files.all[0]
        self.assertEquals(installed.path, manifest.path)
        self.assertEquals(installed.ref, manifest.path)
        self.assertEquals(installed.type_, Files.TYPE.MANIFEST)
        self.assertEquals(installed.data, manifest.dict)
        self.assertEquals(installed.source_url, manifest.uid)

        self.assertEquals(new_f.path, installed.path)
        self.assertEquals(new_f.ref, installed.path)
        self.assertEquals(new_f.type_, installed.type_)

    def test_raises_NotFoundError_if_given_warehouse_not_found(self):
        query = self.sqlite_db.session.query(File)
        all_files = Files(self.sqlite_db, query=query)
        # saving of the file is not important here, so stub it.
        all_files.new_file = fudge.Fake().expects_call()
        all_files._process_source_content = fudge.Fake().expects_call().returns({})

        database = fudge.Fake('database').has_attr(dsn='dsn')
        warehouse = fudge.Fake('warehouse').has_attr(database=database)
        manifest = fudge.Fake().is_a_stub()
        manifest.uid = '1'
        with self.assertRaises(NotFoundError):
            all_files.install_manifest(manifest, warehouse=warehouse)

    @fudge.patch(
        'ambry.orm.File.link_store',
        'ambry.orm.File.link_manifest')
    def test_links_store_to_file_and_manifest_to_store(self, fake_link_store, fake_link_manifest):
        fake_link_store.expects_call()
        fake_link_manifest.expects_call()

        # create warehouse
        FileFactory(path='dsn/path', ref='ref', type_=Files.TYPE.STORE)
        self.sqlite_db.session.commit()

        query = self.sqlite_db.session.query(File)
        all_files = Files(self.sqlite_db, query=query)

        # Source content is not important here, so stub it.
        all_files._process_source_content = fudge.Fake().expects_call().returns({})

        database = fudge.Fake('database').has_attr(dsn='dsn/path')
        warehouse = fudge.Fake('warehouse').has_attr(database=database)
        manifest = fudge.Fake()\
            .has_attr(
                uid=1, dict={}, path='manifest/path')
        manifest.uid = '1'
        all_files.install_manifest(manifest, warehouse=warehouse)

    # .check_query tests
    def test_check_query_raises_object_state_error_if_query_is_empty(self):
        db = None
        f1 = Files(db)
        with self.assertRaises(ObjectStateError):
            f1._check_query()

    @fudge.patch('requests.get')
    def test_process_source_content_reads_remote_site(self, fake_get):
        path = 'http://example.com'
        url_content = 'url_elem1,url_elem2'

        class FakeResponse(object):
            content = url_content

            def raise_for_status(*args, **kwargs):
                pass

        fake_get.expects_call()\
            .returns(FakeResponse())

        db = None
        f1 = Files(db)

        ret = f1._process_source_content(path)
        expected_hash = hashlib.md5(url_content).hexdigest()
        self.assertEquals(ret['hash'], expected_hash)
        self.assertEquals(ret['size'], len(url_content))
        self.assertIn('modified', ret)
        self.assertEquals(ret['content'], url_content)

    def test_process_source_content_reads_file_like_source(self):
        path = ''
        file_like_content = 'file_elem1,file_elem2'
        file_like = StringIO(file_like_content)

        db = None
        f1 = Files(db)

        ret = f1._process_source_content(path, source=file_like)
        expected_hash = hashlib.md5(file_like_content).hexdigest()
        self.assertEquals(ret['hash'], expected_hash)
        self.assertEquals(ret['size'], len(file_like_content))
        self.assertIn('modified', ret)
        self.assertEquals(ret['content'], file_like_content)

    def test_process_source_content_reads_local_file(self):
        path = ''
        f = NamedTemporaryFile(delete=False)
        file_content = 'file_elem1,file_elem2'
        f.write(file_content)
        f.close()

        db = None
        f1 = Files(db)

        try:
            ret = f1._process_source_content(path, source=f.name)
        finally:
            os.remove(f.name)
        expected_hash = hashlib.md5(file_content).hexdigest()
        self.assertEquals(ret['hash'], expected_hash)
        self.assertEquals(ret['size'], len(file_content))
        self.assertIn('modified', ret)
        self.assertEquals(ret['content'], file_content)

    def test_raises_value_error_if_no_content_found(self):
        db = None
        f1 = Files(db)
        path = ''
        source = StringIO('')
        with self.assertRaises(ValueError):
            f1._process_source_content(path, source=source)
