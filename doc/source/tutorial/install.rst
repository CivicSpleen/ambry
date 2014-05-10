.. _install:

################
Installing Ambry
################

Ambry is a complex package that has dependencies on a lot of other code, some of which is hard to build from source, so there is an installation script that automates the installation for OS X and Ubuntu.

********
Mac OS X
********


Although the OS X is the main development platform for Ambry, the OS X installation is a bit difficult, requiring a few outside packages. In particular, it will require:

* XCode, for the system compiler
* Homebrew, to install required binary packages
* The KyngChaos GDAL Complete package, for GDAL, Numpy and Sqlite.


The installation script  will walk you through installing all of these packages, but if the script fails, you may have install them yourself.

Script Install
--------------

To run the automated installer, execute this line from a Terminal:

    $ sh -c "$(https://raw.githubusercontent.com/clarinova/ambry/master/support/install-osx.sh)"

When it detects a missing packages that you have to install manually, the script will pause and open a web browser for you. If those external installs have any hickups, you may have to run the installer script more than once.

Manual Install
--------------

If the script fails, it is usually a problem with installing one of the external packages. You can try to install them outside of the sccript, then run the script to finish.

* For XCode, use the Apple App Store.
* For Homebrew, visit http://brew.sh/
* For the KyngChaos Packages, visit http://www.kyngchaos.com/software/frameworks#gdal_complete


External Package Security
-------------------------

These packages aren't signed, so Mac OS will issue a warning. Use the right-click menu to open them with the installer.

Sometimes the h5py install fails, with the h5py download not being found. This fixes, at a potential security risk:

    pip install --allow-external h5py --allow-unverified h5py h5py


*************
Linux, Ubuntu
*************

For Ubuntu, the script usually works. From a terminal shell, run:


    $ sh -c "$(curl -fsSL https://raw.githubusercontent.com/clarinova/ambry/master/support/install-ubuntu.sh)"

*************
Post Install
*************

The install scripts will also clone example source bundles and insall a basic configuration file. You can verify that the install succeeded with:


.. code-block:: bash

    $ ambry info 
    Version:  0.3.270
    Root dir: /data
    Source :  /data/source
    Configs:  ['/etc/ambry.yaml', '/root/.ambry-accounts.yaml']

    $  ambry library info 
    Library Info
    Name:     default
    Database: sqlite:////data/library.db
    Cache:    FsCache: dir=/data/library upstream=(None)
    Upstream: None
    Remotes:  http://library.clarinova.com

After installation, you can customize the configuation. See: :ref:`configuration`








