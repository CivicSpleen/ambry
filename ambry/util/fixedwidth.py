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

def make_parser(fieldwidths):
    """
    line = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789\n'
    fieldwidths = (2, -10, 24)  # negative widths represent ignored padding fields
    parse = make_parser(fieldwidths)
    fields = parse(line)
    print('format: {!r}, rec size: {} chars'.format(parse.fmtstring, parse.size))
    print('fields: {}'.format(fields))

    :param fieldwidths:
    :return:
    """

    cuts = tuple(cut for cut in accumulate(abs(fw) for fw in fieldwidths))
    pads = tuple(fw < 0 for fw in fieldwidths) # bool values for padding fields
    flds = tuple(izip_longest(pads, (0,)+cuts, cuts))[:-1]  # ignore final one
    parse = lambda line: tuple(line[i:j] for pad, i, j in flds if not pad)
    # optional informational function attributes
    parse.size = sum(abs(fw) for fw in fieldwidths)
    parse.fmtstring = ' '.join('{}{}'.format(abs(fw), 'x' if fw < 0 else 's')
                                                for fw in fieldwidths)
    return parse


def fixed_width_iter(flo, field_widths):

    parser = make_parser(int(w) for w in field_widths)

    for line in flo:
        yield [ e.strip() for e in parser(line) ]

