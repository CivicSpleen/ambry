.. _bundle_structure:

Bundle Structure
================

A Source bundle has at minimum a :file:`bundle.py` file and a :file:`bundle.yaml` file, but usually has many other files. To see the structure of a typical bundle, we'll fetch and build one of the exmaple bundles. 

First, determine where your source directory is. It is usually :file:`~/ambry/source`, but you can find it programtically with:

.. code-block:: bash

    $ ambry source info 
    Source dir: /data/source
    
Change to the source directory, and :command:`git clone` the example bundles: 

.. code-block:: bash

    $ git clone https://github.com/sdrdl/example-bundles.git
    
Change into the directory for the bundle :file:`simple-orig` and you will these files:

.. code-block:: text

    .
    ├── README.md
    ├── bundle.py
    ├── bundle.yaml
    └── meta
        ├── README.md.template
        ├── build.yaml
        ├── doc.yaml
        ├── documentation.md
        ├── partitions.yaml
        ├── schema-old.csv
        ├── schema-revised.csv
        └── schema.csv
        

The two most important files are the bundle files. 

- :file:`bundle.py`: Bundle code.
- :file:`bundle.yaml` Main bundle configuration. 
- :file:`README.md` A README file, primarily for github. Generated from :file:`meta/README.md.template`
        
The meta directory holds a variety of metadata and documentation files. A small number of them are build configuration:

- :file:`meta/build.yaml`: Build specific configuration. Can be empty for very simple bundles. 
- :file:`meta/schema.csv`: A CSV specification for all tables and columns. 

There is one documentation file, and one template for building the README

- :file:`meta/documentation.md`: Detailed documentation, usually changes and errors discovered while creating the bundle. 
- :file:`meta/README.md.template`

THe remaining files are generated during the build, to make introspecting the source bundle easier. 

- :file:`meta/doc.yaml`: A YAML version of the documentation.md file. 
- :file:`meta/partitions.yaml`: A list of all of the partitions created during the build. 

Bundle.yaml
***********

The :file:`bundle.yaml` file is the main configuration for a bundle. It defines the identiy of the bundle, references external documentation and names the creators.  The file will typicall have these sections: 

- about
- contact_bundle
- contact_source
- external_documentation
- identity
- names
- version
 
About section
-------------

.. code-block:: yaml

    about:
        groups:
        - Examples
        license: other-open
        rights: null
        subject: Bundle training
        summary: This is a short summary of the data bundle.
        tags:
        - example
        title: Simple Example Bundle


Contact_bundle and contact_source
---------------------------------
 
.. code-block:: yaml
 
     contact_bundle:
         creator:
             email: bob@bob.com
             name: Bob Bobson
             url: http://example.com
         maintainer:
             email: null
             name: Examplearium
             url: null
     contact_source:
         creator:
             email: info@bobcom.com
             name: Bobcom
             url: https://clarinova.com
         maintainer:
             email: null
             name: null
             url: null
