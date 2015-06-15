"""
"""

from ambry.bundle import Bundle

class Bundle(Bundle):
    """ """

    def build(self):
        import uuid
        import random

        table = 'example'

        p = self.partitions.new_partition(table=table)
        p.clean()

        with p.inserter() as ins:
            for i in range(10000):
                row = dict()

                row['uuid'] = str(uuid.uuid4())
                row['int'] = random.randint(0, 100)
                row['float'] = random.random() * 100

                ins.insert(row)


        return True


