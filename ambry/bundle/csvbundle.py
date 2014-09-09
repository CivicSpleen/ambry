"""A Bundle variant for loading CSV files"""


from  ambry.bundle import BuildBundle

class CsvBundle(BuildBundle):

    def get_source(self, source):
        if not source:
            source = self.metadata.sources.keys()[0]

        fn = self.filesystem.download(source)

        if fn.endswith('.zip'):
            fn = self.filesystem.unzip(fn)

        return fn

    def gen_rows(self, source=None, as_dict=False):
        """Generate rows for a source file. The source value ust be specified in the sources config"""
        import csv

        print dir(csv)

        fn = self.get_source(source)

        with open(fn) as f:

            r = csv.reader(f)

            header =  self.source_header(source)

            if as_dict:
                for row in r:
                    yield dict(zip(header, [None] + row))
            else:
                # It might seem inefficient to return the header every time, but it really adds only a
                # fraction of a section for millions of rows.
                for row in r:
                     yield header, [None] + row

    def source_header(self, source):
        import csv

        fn = self.get_source(source)

        with open(fn) as f:
            r = csv.reader(f)

            return ['id'] + r.next()


    def meta_gen_schema(self, source):

        self.schema.update(source,
                            itr = self.gen_rows(source, as_dict = True),
                            header = self.source_header(source),
                            n = 2000,
                            logger = self.init_log_rate(500))


    def meta(self):

        self.database.create()

        if not self.run_args.get('clean', None):
            self._prepare_load_schema()

        for source in self.metadata.sources:
            self.meta_gen_schema(source)

        return True


    def build(self):


       for source in self.metadata.sources:

           p = self.partitions.find_or_new(table=source)

           p.clean()

           lr = self.init_log_rate(print_rate = 5)

           header = [c.name for c in p.table.columns]

           with p.inserter() as ins:
               for _, row in self.gen_rows(source):
                   lr(str(p.identity.name))

                   ins.insert(dict(zip(header, row)))


           return True

