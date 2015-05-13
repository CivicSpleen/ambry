'''
Example bundle that builds a single partition with a table of random numbers
'''

from ambry.bundle import BuildBundle
 

class Bundle(BuildBundle):
    ''' '''

    def __init__(self, directory=None):
        super(Bundle, self).__init__(directory)

    @property
    def build(self):
        import uuid
        import random

        p = self.partitions.new_partition(table='random1')

        p.query('DELETE FROM random1')

        lr = self.init_log_rate(250)

        with p.database.inserter() as ins:
            for i in range(1000):
                row = {
                    'uuid': str(uuid.uuid4()),
                    'int': random.randint(0, 100),
                    'float': random.random() * 100
                }
                ins.insert(row)
                lr("{} {}".format('random1', i))
               
        #
        # Multiple Partitions for the Second
        #
        
        for grain_n, grain in enumerate(('one', 'two', 'three')):
        
            p = self.partitions.new_partition(table='random2', grain=grain)

            p.clean()

            lr = self.init_log_rate(100)

            with p.database.inserter() as ins:
                # Set initial row ID so that when the segments are loaded into
                # the same table in a warehouse, they have different ids.
                
                ins.row_id = grain_n * 1000
                
                for i in range(1000):
                    row = {
                        'uuid2': str(uuid.uuid4()),
                        'int2': random.randint(0, 100),
                        'float2 ': random.random() * 100
                    }
                    ins.insert(row)
                    lr("{} {}".format(p.grain, i))
        return True
