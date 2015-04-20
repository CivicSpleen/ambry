"""Utilites for working with SAS files."""


#-------------------------------------------------------------------------
# Name:        sas2sqlite
# Purpose:     translate a SAS data set to a SQLite table
# From: https://gist.githubusercontent.com/dapangmao/17b634a94a141e7c8173/raw/gistfile1.py
#-------------------------------------------------------------------------

def sas2sqlite(sasfile, sqlitedb):
    import sqlite3
    from sas7bdat import SAS7BDAT
    # Read data from SAS
    f = SAS7BDAT(sasfile)
    x = f.header.cols
    y = [''] * len(x)

    for i, n in enumerate(x):
        if n[1][2] == "numeric":
            y[i] = n[0] + ' real'
        else:
            y[i] = n[0] + ' varchar({})'.format(n[1][1])

    _table = f.header.dataset.title()
    cmd1 = "CREATE TABLE {} ({})".format(_table, ', '.join(y))
    cmd2 = 'INSERT INTO {} VALUES ( {} )'.format(
        _table,
        ','.join(
            ['?'] *
            len(x)))
    conn = sqlite3.connect(sqlitedb)
    c = conn.cursor()

    for i, line in enumerate(f.readData()):
        if i == 0:
            c.execute('DROP TABLE IF EXISTS {}'.format(_table))
            c.execute(cmd1)
        else:
            c.execute(cmd2, line)
    conn.commit()
    c.close()


#-------------------------------------------------------------------------
# Name:       sas2pd
# Purpose:    import a SAS dataset as a Python pandas dataframe
#
#-------------------------------------------------------------------------

def sas2pd(sasfile):
    import pandas as pd
    from sas7bdat import SAS7BDAT
    a = []

    sf = SAS7BDAT(sasfile)

    for i, x in enumerate(sf.readData()):
        if i == 0:
            cols = x
        else:
            a.append(x)

    df = pd.DataFrame(a)
    df.columns = cols
    return pd.header, df
