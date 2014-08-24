from __future__ import absolute_import
from IPython.core.getipython import get_ipython
from IPython.core.magic import (Magics, magics_class,  cell_magic)
from markdown import markdown

@magics_class
class MarkdownMagics(Magics):

    @cell_magic
    def markdown(self, line, cell):
        from IPython.core.display import HTML


        vars = line.split()

        d = {}
        for k, v in self.shell.user_ns.items():
            if k in vars:
                d[k] = v

        return HTML("<p>{}</p>".format(markdown(cell.format(**d))))


get_ipython().register_magics(MarkdownMagics)

