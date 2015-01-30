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

        for table in ('example', 'example2','example3'):
            
            p = self.partitions.new_partition(table=table)
            p.clean()
            
            with p.database.inserter() as ins:
                for i in range(10000):
                    row = dict()
            
                    row['uuid'] = str(uuid.uuid4())
                    row['int'] = random.randint(0,100)
                    row['float'] = random.random()*100
                    row['year'] = random.randint(0,100)
                    row['hu100'] = random.randint(0,100)
                    row['pop100'] = random.randint(0,100)
                
                    ins.insert(row)

        p = self.partitions.new_partition(table='links')
        p.clean()
        
        with p.database.inserter() as ins:
            for i in range(10000):
                row = dict(example2_id = i, example3_id = i)
                
                ins.insert(row)
            

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
                        
        