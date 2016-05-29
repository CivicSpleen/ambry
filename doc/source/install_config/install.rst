.. _install:


Installing Ambry
################

Ambry is a complex package that has dependencies on a lot of other code, some of which are hard to build from source,
so there are a few different ways to install it. Note that, unless you are installing a dedicated machine, or using a
seperate python installation like Anaconda, you should install Ambry in a virtual environment.

* `Mac OS X`_
* `Linux, Ubuntu`_

Virtual environment
*******************

To use a virtual environment, install the Python virtualenv module. The virtualenv `website has complete instructions
<https://virtualenv.pypa.io/en/latest/installation/>`_, but generally it is as easy as:

First, if you haven't worked in python before, you should install `pip`:

.. code-block:: bash

    $ easy_install pip

Then, install the virtualenv package to create virtual environments.

.. code-block:: bash

    $  pip install virtualenv

Create the virtualenv and activate it:

.. code-block:: bash

    $ virtualenv ambry
    $ source ambry/bin/activate

Setting up and running virtual environments requires remembering a few special commands, so it's more convenient
to use `virtualenvwrapper`, which provides the :command:`workon` command to make using virtualenvironments easier.
See the `documentation for complete instructions <http://virtualenvwrapper.readthedocs.io/en/latest/install.html>`_.


Mac OS X
********


Although it doesn't need to use it to run in all cases, the Ambry imports Postgres modules, so you have to install the
Postgres binaries. On Mac OS X, There are a few ways to do that:

- Install Postgres via Anaconda
- Install Postgres via Homebrew, which also requires XCode to be installed

The easiest way is to get postgres is to install Anaconda, although this will also install a new Python binary.  (
Use Homebrew if you want to avoid using the Anaconda version of Python. ) Visit the `Continuum Analytics downloads page
<http://continuum.io/downloads>` and install it. After installing Anaconda, open a new Terminal window
( an old one won't have the path set correctly. ) In the new terminal window, you should now be running the
python included with Anaconda, which you can check with:

.. code-block:: bash

    $ which python
    /Users/eric/anaconda/bin/python

Since the Anaconda python is seperate from the main system Python, you don't really need to create a virtual environment,
if you are only using Anaconda for Ambry.

Install the psycopg2 package via :command:`conda`, which will also install the postgres binary libraries.

.. code-block:: bash

    $ conda install psycopg2

After installing Postgres, you can install Ambry.

.. code-block:: bash

    pip install ambry


Removing Anaconda
-----------------

Anaconda alters your shell path by adding to the :file:`~/.bash_profile` file. If you edit the file to
remove the statements that Anaconda added, future shells will use the default python.


Linux, Ubuntu
*************



For Ubuntu 14.04 or later,  use this script to install the dependencies and the Ambry package.

.. code-block:: bash

    $ sudo bash -c "$(curl -fsSL https://raw.githubusercontent.com/CivicKnowledge/ambry/master/support/install/install-ubuntu.sh)"

If you've created a fresh Ubuntu install, you'll probably also have to update and install curl. Here's one line that will take care of everything.  

.. code-block:: bash

    $ apt-get update && apt-get install -y curl && \
    sudo bash -c "$(curl -fsSL https://raw.githubusercontent.com/CivicKnowledge/ambry/master/support/install/install-ubuntu.sh)"


Follow up with `Post Install`_ to create your configuration files and check the integrity of the installation. 
   


Post Install
*************
  
After installing Ambry and its dependencies, you can create a default configuration file and check that the installation
worked correctly.
   
.. code-block:: bash
    $ ambry config install
    ...
    $ ambry info 
	Version:   0.3.1612
	Root dir:  /Users/eric/proj/virt/ambry-develop/data
	Source :   /Users/eric/proj/virt/ambry-develop/data/source
	Configs:   ['/Users/eric/proj/virt/ambry-develop/.ambry.yaml']
	Accounts:  /Users/eric/.ambry-accounts.yaml
	Library:   sqlite:////Users/eric/proj/virt/ambry-develop/data/library.db
	Remotes:   test, public

After installation, you can customize the configuation. See: :ref:`configuration`


