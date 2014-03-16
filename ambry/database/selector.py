
"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""


class RowSelector(object):
    """Constructed on a query to a partition, this object allorws rows of a database to be acessed in a
        variety of forms"""

    def __init__(self,partition, sql,*args, **kwargs):
        self.partition = partition
        self.sql = sql
        self.args = args
        self.kwargs = kwargs


    @property
    def numpy(self):
        pass

    @property
    def pandas(self):
        import pandas as pd
        return pd.read_sql(self.sql, self.partition.database.engine.raw_connection(),
                           *self.args, **self.kwargs)


    @property
    def petl(self):
        pass

    @property
    def rows(self):
        pass