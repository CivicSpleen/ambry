__author__ = 'eric'

from IPython.core.getipython import get_ipython
from IPython.core.magic import (
    Magics,
    magics_class,
    line_magic,
    cell_magic,
    line_cell_magic)


@magics_class
class MyMagics(Magics):

    @line_magic
    def lmagic(self, line):
        """my line magic."""
        print line
        print("Full access to the main IPython object:", self.shell)
        print(
            "Variables in the user namespace:",
            list(
                self.shell.user_ns.keys()))
        print dir(self)
        print self.config
        print self.parent
        return line

    @cell_magic
    def cmagic(self, line, cell):
        """my cell magic."""
        return line, cell

    @line_cell_magic
    def lcmagic(self, line, cell=None):
        """Magic that works both as %lcmagic and as %%lcmagic."""
        if cell is None:
            print("Called as line magic")
            return line
        else:
            print("Called as cell magic")
            return line, cell


def load_ipython_extension(ipython):
    print "Loading"


def unload_ipython_extension(ipython):
    print "Unloading"


# In order to actually use these magics, you must register them with a
# running IPython.  This code must be placed in a file that is loaded once
# IPython is up and running:
ip = get_ipython()
# You can register the class itself without instantiating it.  IPython will
# call the default constructor on it.
ip.register_magics(MyMagics)


@magics_class
class StatefulMagics(Magics):

    """Magics that hold additional state."""

    def __init__(self, shell, data):
        # You must call the parent constructor
        super(StatefulMagics, self).__init__(shell)
        self.data = data

    # etc...

# This class must then be registered with a manually created instance,
# since its constructor has different arguments from the default:
ip = get_ipython()
magics = StatefulMagics(ip, 'this is the data')
ip.register_magics(magics)
