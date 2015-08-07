
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

### To run tests:
```bash
$ git clone https://github.com/<githubid>/ambry.git
$ cd ambry
$ pip install -r requirements/dev.txt
$ python setup.py test
```

### To run tests with coverage:

    1. Run with coverage
```bash
$ coverage run setup.py test
```
    2. Generage html:
```bash
$ coverage html
```
    3. Open htmlcov/index.html in the browser.

### To setup PostgreSQL for tests.
Tests use two databases - sqlite and postgresql. SQLite does not require any setup, but PostgreSQL does. You should add postgresql-test section with dsn to the database section of the ambry config. See example:
```yaml
database:
    ...
    postgresql-test: postgresql+psycopg2://ambry:secret@127.0.0.1/
```
Note: Do not include database name to the dsn because each test creates new database empty on each run.

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
