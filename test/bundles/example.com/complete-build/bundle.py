"""
"""

from ambry.bundle import Bundle
from ambry.etl.pipeline import Pipe


class Bundle(Bundle):
    """ """

    def test_coverage(self):
        
        for p in self.partitions:
            print()  # FIXME: print what?
