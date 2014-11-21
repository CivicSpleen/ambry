'''
Example bundle that builds a single partition with a table of random numbers
'''

from  ambry.bundle import BuildBundle
 


class Bundle(BuildBundle):
    ''' '''

    def __init__(self,directory=None):

        super(Bundle, self).__init__(directory)

    def build(self):
        import uuid
        import random

        p = self.partitions.new_partition(table='random1')

        p.query('DELETE FROM random1')

        lr = self.init_log_rate(100)

        with p.database.inserter() as ins:
            for i in range(1000):
                row = {}
                row['uuid'] = str(uuid.uuid4())
                row['int'] = random.randint(0,100)
                row['float'] = random.random()*100

                ins.insert(row)
                lr()

        return True

