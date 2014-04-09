import IPython.nbconvert
from IPython.config import Config
from IPython.nbconvert import HTMLExporter
from IPython.nbformat import current as nbformat
from IPython.nbconvert.preprocessors.base import Preprocessor
from IPython.nbconvert.preprocessors import CSSHTMLHeaderPreprocessor
import argparse
import sys, os

parser = argparse.ArgumentParser(description='Convert an ipython notebook to  HTML')
parser.add_argument('file', metavar='file',  type=str,  help='The file to convert')

args = parser.parse_args()

root_dir =  os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

class AmbryExporter(HTMLExporter):
    
    pass
        
        
class AmbryJSPreprocessor(Preprocessor):
    
    js = []
    
    def __init__(self, config=None, **kw):
        """
        Public constructor
        
        Parameters
        ----------
        config : Config
            Configuration file structure
        **kw : misc
            Additional arguments
        """
        
        super(AmbryJSPreprocessor, self).__init__(config=config, **kw)

        if self.enabled :
            self._regen_header()


    def preprocess(self, nb, resources):
        """Fetch and add Javascript to the resource dictionary

        Fetch CSS from IPython and Pygments to add at the beginning
        of the html files.  Add this css in resources in the 
        "inlining.js" key
        
        WARNING! This must run after the CSSHTMLHeaderPreprocessor to ensure that the
        'inlining' resource key is set up
        
        Parameters
        ----------
        nb : NotebookNode
            Notebook being converted
        resources : dictionary
            Additional resources used in the conversion process.  Allows
            preprocessors to pass variables into the Jinja engine.
        """
      
        resources['inlining']['js'] = self.js
        
        return nb, resources


    def _regen_header(self):
        """ 
        Fills self.header with lines of Javascript extracted from IPython 
        and Pygments.
        """
        
        from pygments.formatters import HtmlFormatter
        
        #Clear existing header.
        header = []
        
        #Construct path to IPy CSS
        from IPython.html import DEFAULT_STATIC_FILES_PATH
        sheet_filename = os.path.join(DEFAULT_STATIC_FILES_PATH,
            'style', 'style.min.css')
        
        #Load style CSS file.
        with io.open(sheet_filename, encoding='utf-8') as file:
            file_text = file.read()
            header.append(file_text)

        #Add pygments CSS
        formatter = HtmlFormatter()
        pygments_css = formatter.get_style_defs(self.highlight_class)
        header.append(pygments_css)

        #Set header        
        self.js = header
        
      
nb_file = args.file

config=Config(
{
    'HTMLExporter':{
        'default_template':'full'
    },
    
}
)


with open(nb_file) as f:
    notebook = nbformat.reads_json(f.read())

## I use basic here to have less boilerplate and headers in the HTML.
## we'll see later how to pass config to exporters.
exportHtml = HTMLExporter(config=config)

exportHtml.register_preprocessor(AmbryJSPreprocessor, enabled=True)

(body,resources) = exportHtml.from_notebook_node(notebook)


out_file = nb_file.replace('.ipynb','.'+resources['output_extension'])

with open(out_file,'w') as f:
    f.write(body.encode('utf8'))

print  resources['inlining'].keys()

#import pprint
#pprint.pprint(dict(resources['inlining']))
