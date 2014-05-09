'''
Example bundle that builds a single partition with a table of random numbers
'''

from  ambry.bundle import BuildBundle
 


class Bundle(BuildBundle):
    ''' '''

    def __init__(self,directory=None):

        super(Bundle, self).__init__(directory)

    def build(self):

        segments = 20

        if int(self.run_args.get('multi')) > 1:

            segs = [seg for seg in range(1, segments)]

            self.run_mp(self.build_segment, segs)

        else:

            for seg in range(1, segments):
                self.build_segment(seg)


    def build_segment(self, seg):
        import uuid
        import random

        p = self.partitions.new_partition(table='rand', segment=seg)

        p.clean()

        lr = self.init_log_rate(100)

        with p.database.inserter() as ins:
            for i in range(100000):
                row = {}
                row['uuid'] = str(uuid.uuid4())
                row['int'] = random.randint(0,100)
                row['float'] = random.random()*100

                ins.insert(row)
                lr("Segment {} ".format(seg))

        return True

