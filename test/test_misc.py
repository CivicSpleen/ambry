"""
Created on Aug 31, 2012

@author: eric
"""
import unittest
from test_base import  TestBase  # @UnresolvedImport

class Test(TestBase):

    def setUp(self):

        pass

    def tearDown(self):
        pass


    def test_lru(self):
        from ambry.util import lru_cache
        from time import sleep

        @lru_cache(maxsize=3)
        def f(x):
            from  random import randint

            return (x,randint(0,1000))


        o =  f(1)
        self.assertEquals(o, f(1))
        self.assertEquals(o, f(1))
        f(2)
        self.assertEquals(o, f(1))
        f(3)
        f(4)
        f(5)
        self.assertNotEquals(o, f(1))


        #
        # Verify expiration based on time.
        #
        @lru_cache(maxtime=3)
        def g(x):
            from  random import randint

            return (x, randint(0, 1000))

        o = g(1)
        sleep(2)
        self.assertEquals(o, g(1))
        sleep(4)
        self.assertNotEquals(o, g(1))

    def test_metadata(self):
        from ambry.bundle.meta import Top, About, Contact, ContactTerm, PartitionTerm, Partitions


        c = Contact()
        c.init('contact',None,None)
        c.creator.name = "Name"
        self.assertIn('publisher', c.dict)
        self.assertIn('name', c.dict['publisher'])
        self.assertIn('name', c.creator.dict)
        self.assertEqual("Name", c.creator.name)

        ct = ContactTerm()
        ct.init('contact', c, None)
        ct.name = "OtherName"
        self.assertIn('name', ct.dict)
        self.assertEqual('OtherName', ct.name)


        d = dict(
            about = dict(
                title = 'title',
                subject = 'subject',
                rights = 'rights',
                summary = 'Summary',
                tags = 'Foobotom'
            ),
            contact = dict(
                creator = dict(
                    name = 'Name',
                    email = 'Email',
                    bingo = 'bingo'
                )
            ),
            # These are note part of the defined set, so aren't converted to terms
            build = dict(
                foo = 'foo',
                bar = 'bar'
            ),
            partitions = [
                dict(
                    name='foo',
                    grain='bar'
                ),
                dict(
                    time='foo',
                    space='bar'
                ),
                dict(
                    name='name',
                    time='time',
                    space='space',
                    grain='grain',
                    segment=0,

                ),
                ]
        )

        top = Top(d)

        #import yaml
        #print yaml.dump(top.dict,default_flow_style=False, indent=4, encoding='utf-8')

        print "ERRORS", top.errors

        def print_key(term):
            print term.path

        #top.visit(print_key)

        self.assertIn('publisher', top.contact.dict)
        self.assertIn('url', top.contact.creator.dict)
        self.assertEqual('Email',top.contact.creator.email)

        self.assertIn('name', top.partitions[0].dict)
        self.assertNotIn('space', top.partitions[0].dict)
        self.assertIn('space', top.partitions[2].dict)
        self.assertEquals('foo', top.partitions[0].dict['name'])


        top.sources.foo.url = 'url'
        top.sources.bar.description = 'description'

        print top.sources.dict

        
def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite
      
if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())