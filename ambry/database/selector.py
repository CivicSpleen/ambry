
"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""


class RowSelector(object):
    """Constructed on a query to a partition, this object allorws rows of a database to be acessed in a
        variety of forms"""

    def __init__(self,partition, sql, index_col = None, *args, **kwargs):
        self.partition = partition
        self.sql = sql
        self.index_col = index_col
        self.args = args
        self.kwargs = kwargs


    @property
    def numpy(self):
        pass

    @property
    def pandas(self):
        import pandas as pd

        if self.index_col:
            def gen():
                header = None
                for i, row in enumerate(self.partition.query(self.sql, *self.args, **self.kwargs)):
                    if i == 0:
                        yield [ k for k,v in row.items() ]

                    yield (row[self.index_col], list(row))
        else:
            def gen():
                for i, row in enumerate(self.partition.query(self.sql, *self.args, **self.kwargs)):
                    yield (i, list(row))


        g = gen()
        header = g.next()

        df =  pd.DataFrame.from_items(g,orient = 'index',columns = header)


        df.convert_objects(convert_dates=True, convert_numeric=True, convert_timedeltas=True, copy=False)

        return df


    @property
    def petl(self):
        pass

    @property
    def rows(self):
        pass