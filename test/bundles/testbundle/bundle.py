"""
Created on Jun 10, 2012

@author: eric
"""
from ambry.bundle import BuildBundle


class Bundle(BuildBundle):
    def prepare(self):

        super(self.__class__, self).prepare()

        return True

    @property
    def fields(self):
        from functools import partial
        import random
        return   [
                  ('id', lambda: None),
                  ('text',partial(random.choice, ['chocolate', 'strawberry', 'vanilla'])),
                  ('integer', partial(random.randint, 0, 500)),
                  ('float', random.random),
                  ('code', partial(random.randint, 0, 10))
                  ]
  
    @property
    def fields2(self):
        from functools import partial
        import random
        return   [
                  ('id', lambda: None),
                  ('text',partial(random.choice, ['chocolate', 'strawberry', 'vanilla'])),
                  ('integer', partial(random.randint, 0, 500)),
                  ('float', random.random),
                  ('extra', lambda: None),
                  ('extra2', lambda: None),
                  ('code', partial(random.randint, 0, 10))
                  ]

        return [
            ('id', lambda: None),
            ('text', partial(random.choice,
                             ['chocolate', 'strawberry', 'vanilla'])),
            ('integer', partial(random.randint, 0, 500)),
            ('float', random.random),
            ('extra', lambda: None),
            ('extra2', lambda: None),
        ]

    @property
    def fields3(self):
        from functools import partial
        import random

        return [
            ('id', lambda: None),
            ('text', partial(random.choice,
                             ['chocolate', 'strawberry', 'vanilla'])),
            ('integer', partial(random.randint, 0, 500)),
            ('float', random.random),
            ('date', lambda: "20{:02d}-{:02d}-{:02d}".format(
                random.randint(0, 10), random.randint(1, 12),
                random.randint(1, 25))),
            ('datetime', lambda: "20{:02d}-{:02d}-{:02d}T{:02d}:{:02d}".format(
                random.randint(0, 10), random.randint(1, 12),
                random.randint(1, 25), random.randint(0, 23),
                random.randint(0, 59))),
            ('time', lambda: "{:02d}:{:02d}".format(random.randint(0, 23),
                                                    random.randint(0, 59)))
        ]

    def build_small(self):
        p = self.partitions.find_or_new_db(table='tthree')
        # table = p.table

        field_gen = self.fields3

        with p.inserter() as ins:
            for i in range(5000):
                row = {f[0]: f[1]() for f in field_gen}
                ins.insert(row)
        return True

    def build(self):



        self.log("=== Build db, using an inserter")
        self.build_db_inserter()

        self.log("=== Build missing")
        self.build_with_missing()

        return True

    def build_with_missing(self):

        p = self.partitions.find_or_new_db(table="tone", grain='missing')

        with p.database.inserter('tone') as ins:
            for i in range(1000):
                ins.insert({ 'tone_id':None,'text':"str"+str(i),'integer':i,'float':i})

    def build_db_inserter_codes(self):
        self.partitions.find_or_new_db(table='coding')


    def build_db_inserter(self):
        
        p = self.partitions.find_or_new_db(table='tthree')

        with self.session:
            table = p.table
            caster = table.caster

        field_gen =  self.fields3
      
        lr = self.init_log_rate(5000)

        with p.inserter() as ins:

            for i in range(10000):
                row = {f[0]: f[1]() for f in field_gen}
                ins.insert(row)
                lr()

            # Should be case insensitive
            for i in range(10000):
                row = {f[0].title(): f[1]() for f in field_gen}
                ins.insert(row)
                lr()

            # The caster should be idempotent
            for i in range(10000):
                row = {f[0]: f[1]() for f in field_gen}
                cast_row, cast_errors = caster(row)
                ins.insert(cast_row)
                lr()


    def deps(self):

        com = self.library.dep('communities').partition

        print com.database.path
