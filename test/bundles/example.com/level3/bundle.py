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

        in_p = self.library.dep('random').partition

        lr = self.init_log_rate()

        for t in ("level3_1", "level3_2"):
            p = self.partitions.new_partition(table = t)

            with p.database.inserter() as ins:
                for row in in_p.rows:
                
                    ins.insert(dict(row))
                    lr()

        return True

