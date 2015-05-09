'''
Example bundle that builds a single partition with a table of random numbers
'''

from ambry.bundle import BuildBundle
 

class Bundle(BuildBundle):
    ''' '''
    def build(self):
        import csv
        
        p = self.partitions.find_or_new(table='combined')

        p.clean()
        
        for k in self.metadata.sources:
            
            fn = self.filesystem.download(k)

            lr = self.init_log_rate(10000)

            header = [c.name for c in p.table.columns]

            with open(fn) as f:
                reader = csv.reader(f)
                with p.inserter() as ins:
                    for row in reader:
                        d = dict(zip(header, row))
                        del d['id']
                        ins.insert(d)
                        lr()

        # References the dependencies so they get into the metadata. 
        
        p = self.library.dep('random')
        p = self.library.dep('simple')

        return True

