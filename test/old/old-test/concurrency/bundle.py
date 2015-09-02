"""
Example bundle that builds a single partition with a table of random numbers
"""

from ambry.bundle import BuildBundle


class Bundle(BuildBundle):
    """ """

    def __init__(self, directory=None):

        super(Bundle, self).__init__(directory)

    def build(self):
        if self.run_args.test:
            self.build_segment(5)

        elif int(self.run_args.get('multi')) > 1:
            segs = [seg for seg in range(1, 100)]

            self.run_mp(self.build_segment, segs)

        else:
            for seg in range(1, 60):
                self.build_segment(seg)

        return True

    def build_segment(self, seg_no):
        import uuid
        import random

        p = self.partitions.find_or_new(table='rand', segment=seg_no)

        p.clean()

        n = 100

        lr = self.init_log_rate(n - 1)

        with p.database.inserter() as ins:
            for i in range(n):
                row = {'uuid': str(uuid.uuid4()), 'int': random.randint(0, 100), 'float': random.random() * 100}

                ins.insert(row)
                lr("seg={}".format(seg_no))

        p.close()

        # To prevent running out of open files in MP mode
        self.close()

        return True

    def x_build_segment(self, seg_no):
        """Create all of the tables for a segment. This will load both
        geographies ( large and small ) and all of the states or one segment"""
        import csv
        import yaml
        from ambry.partitions import Partitions

        tables = self.table_map[seg_no]

        lr = self.init_log_rate(20000)

        raw_codes = []
        for table_name in tables:

            table_name = table_name.lower()

            # We need to convert both the measures and errors files. These have
            # the same structure, but have different prefixes for the file name
            mp = self.partitions.find_or_new(table=table_name, grain='measures')
            ep = self.partitions.find_or_new(table=table_name, grain='errors')

            if mp.state == Partitions.STATE.BUILT and ep.state == Partitions.STATE.BUILT and not self.run_args.test:
                self.log("Partition {} is already built".format(mp.identity.sname))
                return

            try:
                p.execute("DELETE FROM {}".format(table_name))
            except:
                pass

            table = self.schema.table(table_name)

            with self.session:
                header = [c.name for c in table.columns]

            for stusab, state in list(self.states.items()):

                if self.run_args.test and stusab != 'CA':
                    continue

                for geo in ('large', 'small'):
                    url = self.build_get_url(geo, stusab, seg_no)

                    mfn, efn = self.download(url)

                    for fn, p in [(efn, ep), (mfn, mp)]:
                        # self.log("{} {}".format(fn, p.vname))
                        with open(fn) as f:
                            reader = csv.reader(f)

                            with p.inserter() as ins:
                                for line in reader:
                                    lr("{} {} {} {}".format(
                                        table_name, stusab, geo,
                                        p.identity.grain))
                                    d = dict(list(zip(header, line)))

                                    errors = ins.insert(d)

                                    if errors:
                                        raw_codes.append(
                                            (line[:6], errors, geo, table.name))

        # Write out the column names in each table, segment, that the Caster
        # could not translate. These should get processed into meta information
        # that will add 'code' columns into tables to hold the orig values of
        # the Jam Codes
        code_cols = {}
        for link, errors, geo, table in raw_codes:
            ld = dict(list(zip(header[:6], link)))
            del ld['fileid']
            del ld['filetype']  # Should aways be 2012e5
            ld['geo'] = geo
            ld['table'] = table

            if table not in code_cols:
                code_cols[table] = []

            for k, v in list(errors.items()):
                ld['col'] = k
                ld['value'] = v

                if k not in code_cols[str(table)]:
                    code_cols[table].append(str(k))

        if len(code_cols):
            with open(self.filesystem.path(
                    'build', 'code', 'codes-{}.yaml'.format(seg_no)), 'w') as f:
                f.write(yaml.dump(code_cols, indent=4,
                                  default_flow_style=False))
