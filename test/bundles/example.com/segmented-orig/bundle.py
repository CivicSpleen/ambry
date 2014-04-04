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

        
        lr = self.init_log_rate(print_rate=5)
        
        
        for j in range(4):
            p = self.partitions.new_partition(table='example', segment=j)
            p.query('DELETE FROM example')
            nd = p.table.null_dict
            with p.database.inserter() as ins:
                for i in range(1000):
            
                    row = dict(nd.items())
        
                    row['uuid'] = str(uuid.uuid4())
                    row['int'] = random.randint(0,100)
                    row['float'] = random.random()*100
    
                    ins.insert(row)
                    lr()
        
        return True

