"""
Support for creating web pages and text representations of schemas.
"""
from genshi.template import TemplateLoader
import os 

class Web(object):
    
    def __init__(self, bundle):
        self.bundle = bundle
        
        self.loader = TemplateLoader(os.path.join(os.path.dirname(__file__), 'support', 'templates'),auto_reload=True)
        
    def schema_table(self, table_name):
        
        table = self.bundle.schema.table(table_name)
        
        tmpl = self.loader.load('schema_table.html')
        return tmpl.generate(bundle=self.bundle,
                             schema=self.bundle.schema,
                             config=self.bundle.config,
                             table=table
                             ).render('html', doctype='html')
        