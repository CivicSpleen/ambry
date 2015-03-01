'''

'''

from  ambry.bundle import BuildBundle
 

class Bundle(BuildBundle):
    ''' '''

    def __init__(self,directory=None):

        super(Bundle, self).__init__(directory)

    def build(self):
        from ambry.warehouse import  new_warehouse
        from ambry.warehouse.manifest import Manifest
        import sqlite3
        
        wf  = self.filesystem.build_path(self.identity.vname, 'warehouse.db')

        w = new_warehouse('sqlite:///{}'.format(wf), self.library, self.logger )

        self.log("Installing manifest to {}".format(w.database.dsn))

        w.create()

        w.install_manifest(Manifest(self.filesystem.meta_path('manifest.ambry')))
        
        self.log("Installed manifest to {}".format(w.database.dsn))
        
        for row in w.database.connection.execute("SELECT * FROM example_combined"):
            print row

        return True
        

