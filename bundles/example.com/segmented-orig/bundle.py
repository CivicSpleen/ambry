'''
'''

from  ambry.bundle import BuildBundle
 

class Bundle(BuildBundle):
    ''' '''
 
    def __init__(self,directory=None):
        
        super(Bundle, self).__init__(directory)
 
    def build(self):
        import uuid
        import random

        p = self.partitions.new_partition(table='example')
        
        p.query('DELETE FROM example')
        nd = p.table.null_dict
        
        lr = self.init_log_rate(print_rate=5)
        
        with p.database.inserter() as ins:

            for i in range(10000):
                row = dict(nd.items())
            
                row['uuid'] = str(uuid.uuid4())
                row['int'] = random.randint(0,100)
                row['float'] = random.random()*100
        
                ins.insert(row)
                lr()
        
        p.csvize(rows_per_seg=2500)
        
        return True

