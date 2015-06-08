
Developer's Notes
=================

To build and upload to pypi:

    $ python setup.py sdist upload

or

    $ python setup.py sdist bdist_wininst upload


The versino number is based on the number of git commits, and is set with a pre-commit hokm which you have to setup
manually for each new cloned repo: 

    $ cp support/pre-commit.sh .git/hooks/pre-commit
    $ chmod 775 .git/hooks/pre-commit

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
