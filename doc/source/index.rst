
Ambry Data Management
=====================

Ambry is a data management system for packaging, storing and referencing research datasets, with an emphasis on public data. Using Ambry, you can create packages of data, in a similar way that source code packages can be created using tools like Apt, Homebrew, Bower or doezns of others. These pckages can be stored in libraries and installed to databases and data repositories. See the :ref:`Overview <about_overview>` for examples of use. 

In Ambry, data is packaged into Bundles, with each Bundle being composed of one or more partitions. The partitions are segmented along natural divisions in the data. For instance, with educations data, there is usually one partition per year, while for Census data, each year is a separate bundle, and there is one partition per state. The partition is a single-file relational database ( a Sqlite database ) and usually contains only one table.

Bundles and Partitions can be loaded into Libraries. A Library synchronizes with a web-based repository for bundles and partitions, and users can synchronize a library with one or more repositories. Once a Library is synchronized, the user can query the library to look for bundles and partitions. After finding interesting partitions, the user can install the partitions to a warehouse, which is a relational-database where the data from the partitions can be accessed through a SQL connection to the database, or though file extracts, accessed through a web application.

Contents:

.. toctree::
    :maxdepth: 1
    
    tutorial/install
    about/index
    tutorial/index
    cli/index
    tutorial/tips
    recipes/index
    source/index
    warehouse/index    

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

