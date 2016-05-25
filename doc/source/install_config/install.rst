.. _install:


Installing Ambry
################

Ambry is a complex package that has dependencies on a lot of other code, some of which are hard to build from source, so there are a few different ways to install it. 

* `Mac OS X`_
* `Linux, Ubuntu`_


.. _install-mac:

Mac OS X
********


Easy Way
--------

The easiest way to install ambry is to for install the scientific python distribution, Anaconda. Visit the `Continuum Analytics downloads page <http://continuum.io/downloads>`_ and get the Anaconda installation for your distribution.

After installing Anaconda, open a new Terminal window ( an old one won't have the path set correctly. ) You should now be running the python included with anaconda:
 
.. code-block:: bash

    $ which python 
    /Users/eric/anaconda/bin/python

 
Finally, install Ambry with pip:
    
.. code-block:: bash

    $ pip install ambry

Now see `Post Install`_ to create your configuration files and check the integrity of the installation. 
    


Linux, Ubuntu
*************

For Ubuntu 13.04 through 14.04, use this script to install the dependencies and the Ambry package.

.. code-block:: bash

    $ sudo bash -c "$(curl -fsSL https://raw.githubusercontent.com/CivicKnowledge/ambry/master/support/install/install-ubuntu-14.04.sh)"

If you've created a fresh Ubuntu install, you'll probably also have to update and install curl. Here's one line that will take care of everything.  

.. code-block:: bash

    $ apt-get update && apt-get install -y curl && \
    sudo bash -c "$(curl -fsSL https://raw.githubusercontent.com/CivicKnowledge/ambry/master/support/install/install-ubuntu-14.04.sh)"


Follow up with `Post Install`_ to create your configuration files and check the integrity of the installation. 
   



Post Install
*************
  
After installing Ambry and its dependencies, you can check that the installation worked correctly with:
   
.. code-block:: bash
    
    $ ambry info 
	Version:   0.3.1612
	Root dir:  /Users/eric/proj/virt/ambry-develop/data
	Source :   /Users/eric/proj/virt/ambry-develop/data/source
	Configs:   ['/Users/eric/proj/virt/ambry-develop/.ambry.yaml']
	Accounts:  /Users/eric/.ambry-accounts.yaml
	Library:   sqlite:////Users/eric/proj/virt/ambry-develop/data/library.db
	Remotes:   test, public

After installation, you can customize the configuation. See: :ref:`configuration`


