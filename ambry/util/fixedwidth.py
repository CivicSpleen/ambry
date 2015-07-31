# From : http://stackoverflow.com/a/4915359


try:
    from itertools import izip_longest  # added in Py 2.6
except ImportError:
    from itertools import zip_longest as izip_longest  # name change in Py 3.x

try:
    from itertools import accumulate  # added in Py 3.2
except ImportError:
    def accumulate(iterable):
        'Return running totals (simplified version).'
        total = next(iterable)
        yield total
        for value in iterable:
            total += value
            yield total


def fixed_width_iter(flo, source):

    parts = []
    for i, c in enumerate(source.source_table.columns):

        try:
            int(c.start)
            int(c.width)
        except TypeError:
            raise TypeError("Source table {} must have start and width values for {} column "
                            .format(source.source_table.name, c.source_header))

        parts.append("row[{}:{}]".format(c.start-1,c.start+c.width-1))

    parser = eval("lambda row: [{}]".format(','.join(parts)))

    yield source.source_table.headers

    for line in flo:
        yield [ e.strip() for e in parser(line.strip()) ]
