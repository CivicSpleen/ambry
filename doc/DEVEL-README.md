
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
Tests use two databases - sqlite and postgresql. SQLite does not require any setup, but PostgreSQL does. You should add postgresql account with dsn to the your accounts file. See example:
```yaml
accounts:
    ...
    postgresql:
        dsn: postgresql+psycopg2://ambry:secret@127.0.0.1/ambry
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
