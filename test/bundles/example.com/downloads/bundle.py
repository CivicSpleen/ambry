""""""

from ambry.bundle import BuildBundle


class Bundle(BuildBundle):

    """"""

    def __init__(self, directory=None):

        super(Bundle, self).__init__(directory)

    def build(self):

        self.filesystem.download('example',force = True)

        return True
