
Developer's Notes
=================

To build and upload to pypi:

    $ python setup.py sdist upload

or

    $ python setup.py sdist bdist_wininst upload


To run tests:

    $ git clone https://github.com/<githubid>/ambry.git
    $ cd ambry
    $ pip install -r requirements/dev.txt
    $ python setup.py test

To run tests with coverage:

    1. Run with coverage
      $ coverage run setup.py test
    2. Generage html:
      $ coverage html
    3. Open htmlcov/index.html in the browser.

To change model's fields and appropriate database table (AKA migration):

    1. Change appropriate models.
    2. Make new empty migration
      $ ambry makemigration <your_migration_name>
    3. Open file created by command and add appropriate sql to the _migrate_sqlite and _migrate_postgresql methods.
    4. Set is_ready to True.
    5. Increment orm.database.SCHEMA_VERSION (now your SCHEMA_VERSION has to match to migration number.) # FIXME: Do you really need SCHEMA_VERSION? If ambry sees missed migrations, just apply them.
    6. Check just created migration on your own db.  (FIXME: Describe how.)
    7. Commit and push file created by command.
