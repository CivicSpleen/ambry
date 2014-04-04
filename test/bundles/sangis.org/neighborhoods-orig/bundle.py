'''
'''

from  ambry.bundle.geo import GeoBuildBundle

class Bundle(GeoBuildBundle):
    ''' '''
    pass


    def add_views(self):
        
        for p in self.partitions:

            if not p.table:
                continue
  
            views = self.config.views.get(p.table.name, False)
 
            if not views:
                continue
            
            for name, view in views.items():
                self.log("Adding view: {} to {}".format(name, p.identity.name))
                sql = "DROP VIEW IF EXISTS {}; ".format(name)
                p.database.connection.execute(sql)
                  
                sql = "CREATE VIEW {} AS {};".format(name, view)
                p.database.connection.execute(sql)           
            
