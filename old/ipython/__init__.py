
"""Copyright (c) 2014 Clarinova.

This file is licensed under the terms of the Revised BSD License,
included in this distribution as LICENSE.txt

"""

from ambry import library as _lf


def library(name='default'):
    """Return the default Analysislibrary for this installation, which is like
    the Library returned by library(), but configured for use in IPython."""
    from ambry.library import AnalysisLibrary

    return AnalysisLibrary(_lf())


def get_ipython_notebook_path():
    """Return the path to the current notebook file.

    This is a horrible hack, but there doesn't seem to be another way to
    do it.

    """

    # Partly from: http://stackoverflow.com/a/13055551/1144479

    from IPython.core.getipython import get_ipython
    import requests
    import os
    from IPython.lib import kernel
    cf = kernel.get_connection_file()

    kernel_id = cf.split('-', 1)[1].split('.')[0]

    r = requests.get('http://127.0.0.1:8888/api/sessions')
    r.json()

    notebook_path = None
    for s in requests.get('http://127.0.0.1:8888/api/sessions').json():
        if s['kernel']['id'] == kernel_id:
            notebook_path = os.path.join(
                get_ipython().starting_dir,
                s['notebook']['path'],
                s['notebook']['name'])
            break

    return notebook_path


def get_ipython_server_info():
    """Return the notebook server info as a dict, using an egregious hack.

    This will be correct only if there is only one IPython notebook
    instance running for each get_ipython().starting_dir

    """

    import json
    import os
    from IPython.core.getipython import get_ipython
    from IPython.lib import kernel
    cf = kernel.get_connection_file()

    root_dir = get_ipython().starting_dir

    security_dir = os.path.dirname(cf)

    for file in os.listdir(security_dir):
        if file.startswith("nbserver"):
            with open(os.path.join(security_dir, file)) as f:
                d = json.loads(f.read())
                print d
                if d and d.get('notebook_dir') == root_dir:
                    return d
