""""
These are random tests used in development. They aren't meant to be comprehensive or to exercise any specific bugs. """

from test.test_orm.base import BasePostgreSQLTest, MISSING_POSTGRES_CONFIG_MSG
import unittest

from ambry.bundle import Bundle

class Test(BasePostgreSQLTest):


    def test_sequence(self):
        from sqlalchemy import Sequence

        con = self.pg_connection()

        seq = Sequence('some_sequence')

        for i in range(10):
            nextid = con.execute(seq)
            print nextid.next_number('foobar')

