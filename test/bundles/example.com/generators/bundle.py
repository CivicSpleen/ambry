"""
"""

from ambry.bundle import Bundle


class RandomSourcePipe(object):

    def __init__(self, bundle, source):

        self.year = int(source.time)
        self.space = source.space

    def __iter__(self):

        import uuid
        from collections import OrderedDict

        for i in range(200):
            row = OrderedDict()

            row['uuid'] = str(uuid.uuid4())
            row['number'] = i
            row['number2'] = i*2

            if i == 0:
                yield row.keys()

            yield row.values()


class Bundle(Bundle):
    """ """

    def check_subclass(self):
        return True

    def build(self, stage='main', sources=None, force = False):

        self.pre_build()

        self.phase_main('build', sources=sources, stage='main')

        self.phase_main('build', sources=sources, stage='build2')

        self.post_build()

        return True



