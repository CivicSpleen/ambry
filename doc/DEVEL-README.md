
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
      $ coverage run python setup.py test
    2. Generage html:
      $ coverage html
    3. Open htmlcov/index.html in the browser.
