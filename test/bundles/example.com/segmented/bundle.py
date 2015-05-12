"""
"""

from ambry.bundle import BuildBundle


class Bundle(BuildBundle):
    """ """

    records_per_segment = 5000

    def __init__(self, directory=None):

        super(Bundle, self).__init__(directory)

    def build(self):
        import uuid
        import random

        lr = self.init_log_rate(N=2000)

        for seg in range(1, 4):

            p = self.partitions.find_or_new(table='example', segment=seg)
            p.clean()
            nd = p.table.null_dict

            with p.database.inserter() as ins:

                ins.row_id = (seg - 1) * self.records_per_segment

                for i in range(self.records_per_segment):
                    row = dict(nd.items())

                    row['uuid'] = str(uuid.uuid4())
                    row['int'] = random.randint(0, 100)
                    row['float'] = random.random() * 100

                    ins.insert(row)
                    lr("Seg {}".format(seg))

        return True

