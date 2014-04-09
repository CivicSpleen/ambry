#! /usr/bin/env python 
# Convert an IPython notebook to HTML
import IPython.nbconvert
from IPython.config import Config
from IPython.nbconvert import HTMLExporter
from IPython.nbformat import current as nbformat
from IPython.nbconvert.preprocessors.base import Preprocessor
from IPython.nbconvert.preprocessors import CSSHTMLHeaderPreprocessor
import argparse, sys, os, datetime

def modification_date(filename):
    t = os.path.getmtime(filename)
    return datetime.datetime.fromtimestamp(t)

parser = argparse.ArgumentParser(description='Convert an ipython notebook to  HTML')
parser.add_argument('file', metavar='file',  type=str,  help='The file to convert')

args = parser.parse_args()

root_dir =  os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

nb_file = args.file
nb_md = modification_date(nb_file)

out_file = nb_file.replace('.ipynb','.html')

if os.path.exists(out_file):
    out_md = modification_date(out_file)

    if out_md > nb_md:
        sys.exit(0) 

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

(body,resources) = exportHtml.from_notebook_node(notebook)


with open(out_file,'w') as f:
    f.write(body.encode('utf8'))

print  out_file