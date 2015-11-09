import sys
from os.path import dirname

from ambry.run import get_runconfig
from ambry.library import new_library

rc = get_runconfig()
rc.group('filesystem')['root'] = rc.group('filesystem')['test']
l = new_library(rc, 'test')


l.import_bundles(dirname(__file__), detach = True)

for b in l.bundles:
    b = b.cast_to_subclass()
    b.run_stages()