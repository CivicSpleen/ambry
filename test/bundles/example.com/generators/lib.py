from ambry.etl import SourcePipe

class RandomSourcePipe(SourcePipe):

    def __init__(self, source, cache_fs, account_accessor):
        super(RandomSourcePipe, self).__init__(source, cache_fs, account_accessor)

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
