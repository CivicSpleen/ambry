"""Support for Jupyter notebooks

Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

from IPython.core.magic import (register_line_magic, register_cell_magic,
                                register_line_cell_magic)

@register_cell_magic
def warehouse_query(line, cell):
    "my cell magic"
    from IPython import get_ipython

    parts = line.split()
    w_var_name = parts.pop(0)
    w = get_ipython().ev(w_var_name)

    w.query(cell).close()