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
            for i in range(100000):
                row = dict(nd.items())
            
                row['uuid'] = str(uuid.uuid4())
                row['int'] = random.randint(0,100)
                row['float'] = random.random()*100
        
                ins.insert(row)
                lr()
        
        return True
        
    def build_add_codes(self):
        
        code_key = 0
        
        for tn in ('example2', 'example3'):
            t  = self.schema.table(tn)
        
            with self.session:
                for c in t.columns:
                    if c.datatype in (c.DATATYPE_INTEGER, c.DATATYPE_FLOAT) and c.name != 'id':
                        code_key += 1
                        cd = c.add_code(code_key, 'code val {}'.format(code_key))
                        
        
            
        

