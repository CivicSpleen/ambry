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

from ambry.library import files
from ambry.dbexceptions import ObjectStateError


class FilesTest(unittest.TestCase):

    def test_all_property_returns_all_records(self):
        # TODO: return the same value as SQLAlchemy returns
        all_result = [['elem1'], ['elem2']]
        query = fudge.Fake()\
            .expects('all')\
            .returns(all_result)
        db = None  # does not matter right now
        f1 = files.Files(db, query)
        self.assertEquals(f1.all, all_result)

    def test_first_property_returns_first_record(self):
        # TODO: return the same value as SQLAlchemy returns
        first_result = ['elem1']
        query = fudge.Fake()\
            .expects('first')\
            .returns(first_result)
        db = None  # does not matter right now
        f1 = files.Files(db, query)
        self.assertEquals(f1.first, first_result)

    def test_one_property_returns_single_record(self):
        # TODO: return the same value as SQLAlchemy returns
        one_result = ['elem1']
        query = fudge.Fake()\
            .expects('one')\
            .returns(one_result)
        db = None  # does not matter right now
        f1 = files.Files(db, query)
        self.assertEquals(f1.one, one_result)

    def test_one_maybe_returns_single_record(self):
        # TODO: return the same value as SQLAlchemy returns
        one_result = ['elem1']
        query = fudge.Fake()\
            .expects('one')\
            .returns(one_result)
        db = None  # does not matter right now
        f1 = files.Files(db, query)
        self.assertEquals(f1.one_maybe, one_result)

    def test_one_maybe_returns_none_if_there_is_no_record(self):
        # TODO: return the same value as SQLAlchemy returns
        query = fudge.Fake()\
            .expects('one')\
            .raises(NoResultFound)
        db = None  # does not matter right now
        f1 = files.Files(db, query)
        self.assertEquals(f1.one_maybe, None)

    def test_order_returns_instance_with_ordered_query(self):
        order_column = 1
        query = fudge.Fake()\
            .expects('order_by')\
            .with_args(order_column)
        db = None  # does not matter right now
        f1 = files.Files(db, query)
        self.assertEquals(f1.order(order_column), f1)

    def test_delete_deletes_all_records(self):
        query = fudge.Fake()\
            .expects('delete')\
            .expects('count')\
            .returns(1)
        db = fudge.Fake()\
            .expects('commit')
        f1 = files.Files(db, query)
        self.assertIsNone(f1.delete())

    def test_update_updates_records_in_the_query(self):
        # TODO: change record format to match to the real value.
        records = ['1']
        query = fudge.Fake()\
            .expects('update')\
            .with_args(records)
        db = None
        f1 = files.Files(db, query)
        self.assertIsNone(f1.update(records))

    def test_check_query_raises_object_state_error_if_query_is_empty(self):
        db = None
        f1 = files.Files(db)
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
        f1 = files.Files(db)

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
        f1 = files.Files(db)

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
        f1 = files.Files(db)

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
        f1 = files.Files(db)
        path = ''
        source = StringIO('')
        with self.assertRaises(ValueError):
            f1._process_source_content(path, source=source)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(FilesTest))
    return suite

if __name__ == '__main__':
    unittest.TextTestRunner().run(suite())
