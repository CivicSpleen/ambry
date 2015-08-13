import ambry.bundle 


class Bundle(ambry.bundle.Bundle):
    
    def edit_meta_pipeline(self, pl):
        from ambry.etl.pipeline import PrintRows, augment_pipeline
        
        augment_pipeline(pl, PrintRows)
        
