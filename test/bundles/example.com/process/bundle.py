import ambry.bundle 


class Bundle(ambry.bundle.Bundle):
    
    def edit_meta_pipeline(self, pl):
        from ambry.bundle.etl.pipeline import PrintRows, augment_pipeline
        
        augment_pipeline(pl, PrintRows)
        
        
    def build(self):
        from ambry.bundle.etl.pipeline import sink
        
        pl = sink(self.do_meta_pipeline('types3'))

        print str(pl)
            
        return True