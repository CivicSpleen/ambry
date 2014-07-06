"""Classes for converting warehouse databases to other formats.

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

def extract(database, table, format, cache, dest):

    from ambry.warehouse.extractors import CsvExtractor


    ex = dict(
        csv=CsvExtractor()
    ).get(format, False)


    if not ex:
        raise ValueError("Unknown format name '{}'".format(format))

    row_gen = database.connection.execute("SELECT * FROM {}".format(table))

    ex.extract(row_gen, cache.put_stream(dest))

    return cache.path(dest)

class CsvExtractor(object):

    def __init__(self):
        pass

    def extract(self, row_gen, stream):

        import unicodecsv
        w = unicodecsv.writer(stream)

        for i,row in enumerate(row_gen):
            if i == 0:
                w.writerow(row.keys())

            w.writerow(row)



