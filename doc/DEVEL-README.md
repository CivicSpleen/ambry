
Developer's Notes
=================

### To build and upload to pypi:
```bash
$ python setup.py sdist upload
```

or
```bash
$ python setup.py sdist bdist_wininst upload
```

### To change model's fields and appropriate database table (AKA migration):
    1. Change appropriate models.
    2. Make new empty migration
```bash
$ ambry makemigration <your_migration_name>
```
    3. Open file created by command and add appropriate sql to the _migrate_sqlite and _migrate_postgresql methods.
    4. When you are ready to apply migration, increment orm.database.SCHEMA_VERSION (now your SCHEMA_VERSION has to match to migration number.)
    5. Check just created migration on your own db (check both, SQLite and PostgreSQL). You can apply your migration by running:
```bash
$ ambry list
```
    6. Commit and push files your changed.

### Setup environment to run ambry under python-3 (debian).
    1.  Install python 3
```bash
    $ apt-get install python3 python3-doc python3-dev
```
    2. Setup a new virtualenv with python3
```
$ mkvirtualenv --python=/usr/bin/python3 ambry3
```
    3. Check virtualenv python version
```bash
 $ python -V
Python 3.4.2
```

### Foreign Data Wrapper (PostgreSQL) / Virtual tables (SQLite)
#### SQLite
Install python-apsw (sqlite virtual tables support).
python-apsw requires at least sqlite v 3.8.8. Some systems may have lower version, in such case
you can install private version of the sqlite for the python-apsw.
    1. Download apsw source code.
        http://rogerbinns.github.io/apsw/download.html
    2. Unzip it, cd to unzipped directory.
    3. Install
    4. If you use virtualenv copy apsw.so to the site-packages directory of the virtual env.
```bash
python setup.py fetch --all build --enable-all-extensions install test
```
See http://apidoc.apsw.googlecode.com/hg/build.html#recommended for more details.

#### PostgreSQL (Foreign Data Wrapper), Debian/Ubuntu, virtualenv
1. Install postgresql-server-dev package
```bash
$ sudo apt-get install postgresql-server-dev-(your version here)
```
2. Install multicorn from source code *to global environment*.
```bash
$ sudo pgxn install multicorn
$ wget https://github.com/Kozea/Multicorn/archive/v1.2.3.zip
$ unzip v1.2.3.zip
$ cd Multicorn-1.2.3
$ make && make install
```
3. Install ambryfdw *to global environment*.
```bash
pip install ambryfdw
```
4. Create \*.pth files for both libraries in the site-packages of your virtual environment.
Add multicorn.pth containing path to the multicorn package. Example (use your own path instead):
```
/usr/local/lib/python2.7/dist-packages/multicorn-1.2.3_dev-py2.7-linux-i686.egg
```
Add ambryfdw.pth containing path to the ambryfdw package. Example (use your own path instead):
```
/usr/local/lib/python2.7/dist-packages/ambryfdw
```

### To write python2/python3 compatible code:

Ambry uses one code base for both - python2 and python3. This requires some extra work.

    1. Tests should run in both - python2 and python3. FIXME: Not ready yet.
    
    2. Run 2to3 before push for all files you changed. FIXME: Not ready yet.

Usefull hints:

    1. Use print() instead of print. For complicated cases (print('', file=sys.stderr) for example) use six.print_.
    
    2. Use six.iteritems, six.iterkeys, six.itervalues instead of {}.iteritems, {}.iterkeys, {}.itervalues if you need iterator.
    
    3. If you need lists from {}.keys, {}.values wrap both with list - list({}.keys()), list({}.values())
    
    4. Use urlparse from six:
```python
from six.moves.urllib.parse import urlparse
```
instead of (py2 style)
```python
from urlparse import urlparse
```

    5. Use StringIO from the six package:
```python
from six import StringIO
```
instead of (py2 style)
```python
from StringIO import StringIO
```

    6. Use filterfalse from the six package:
```python
from six import filterfalse
```
instead of (py2 style)
```python
from itertools import ifilterfalse
```

    7. Use six.string_types to check for string:
```python
isinstance(value, string_types)
```
instead of (py2 style)
```python
isinstance(value, basestring)
```

    8. Use six.moves.builtins for builtins access:
```python
from six.moves import builtins
```
instead of (py2 style)
```python
import __builtins__
```

    9. Use \__bool__ instead of \__nonzero__. For python2 compatibility use next hack:
```python
class Foo(object):
    def __bool__(self):
        return bool(...)

Foo.__nonzero__ = Foo.__bool__
```
Doing so prevents 2to3 to replace \__nonzero__.

    10. Use six.u() if you need unicode, use six.b() if you need bytestring:
```python
u('some-str')
```
instead of
```python
u'some-str'
```

    11. Use six.u() if you need to call \__unicode__. Use six.b() if you need to call \__str__:
```python
u('{}').format(instance)
```
instead of
```python
unicode(instance)
```
and
```python
b('{}').format(instance)
```
instead of
```python
str(instance)
```

    12. Use six.text_type and six.binary_type instead of str and unicode types.
```python
types = [int, six.binary_type]
```
instead of
```python
types = [int, str]
```
and
```python
types = [int, six.text_type]
```
instead of
```python
types = [int, unicode]
```

# Setting up Docker Environment

## Postgres

    $ docker run  \
    -e POSTGRES_USER=ambry \
    -e POSTGRES_PASSWORD=ambry \
    -P  -d postgres --name ambry

The config file in the container is: /var/lib/postgresql/data/postgresql.conf.

To connect to the database, you'll use a DSN URL that has the port that docker picted to make the internal Postgres 
port to, which you can get with `docker ps` or `docker port ambry 5432`
