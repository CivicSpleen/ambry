.. _configure_bundle:

Configure a Bundle
==================

Configuring a bundle for building involves setting the metadata, sources, dependencies and requirements, and for many basic datasets, selecting the right Bundle class. 

* Basic metadata
* Select a bundle class
* Set source and documentation links
* Run meta phase, or write Schema. 


Basic Metadata
**************

Open the :file:`bundle.yaml` file to edit the basic metadata. 

The most important values to set are: 

- about.title
- about.summary
- external_documentation.dataset

First, find the main web page for the dataset. This should be a page that has the title of the dataset and some basic overview information about it, the first page you are likely to find, and a page that links to other pages about the dataset. Describe this page and set its URL in the ``external_documentation.dataset`` variable. 

If there are additional pages associated with the dataset, set ``external_documentation.download`` to the page that containts links to download files, but remove the entire section if it is the same page as for ``external_documentation.dataset``. 

If there is a seperate page or PDF for the dataset documentation, set it in ``external_documentation.documentation``, but remove the section if this information does not exist, or already exists on the ``external_documentation.dataset`` page. 

The Title and Summary can usually be copied directly from the ``external_documentation.dataset`` page, but if not, they should be written carefully, as they are used to describe the dataset thoughout the dataset's documentation. 

Fill in other  ``about`` entries as appropriate: 

- about.license: 'public' if there is no declared license, otherwise the name or, better a URL, to the license. 
- about.processed: A statement about how the data were processed, if there were substantial changes from the original sources
- about.footnote: A statement about unusual interpretations or meaning of the dataset, or anything else that would be approprate in a footnote. 
- about.time: A year or year range that the dataset covers. Seperate start and end years with a '/'
- about.space: The name of the state or county that the data cover, or "US" for national data. Must be a term that returns a hit from the search function :command:`ambry search -I`
- about.access: A name of a repote library where the built bundle will be uploaded. Usually "public"


Source and Documentation Links
******************************

Once you've setup the basic metadata, and in particular, set the download or documentation values, you can set the source URLS. These are references to the source files that will be loaded into the bundle. 

The easiest way to get these links is to run :command:`bambry config scrape`. This will extract the links from the pages specified by ``external_documentation.download`` and ``external_documentation.dataset``, looking for PDF, CSV and XLS files. It will dump the links in the proper formats for the ``sources`` and ``external_documentation`` sections. XLS and CSV files will go in the sources section, while PDF files will go in the external_documentation section. 

You can often just copy these into the configuration. The sources go into the ``sources`` section in the :file:`meta/build.yaml` file. You can also copy in the exteral_documentation values, but it's usually better to only copy the most important ones, since users will usually prefer to use the links from the original page, rather than from the Ambry documentation. 


Select Bundle Class
*******************

If you will be building from one or more CSV, Excel or TSV files, edit the :file:`bundle.py` to change the base class to one from :py:mod:`ambry.bundle.loader`.

- :py:class:`ambry.bundle.loader.CsvBundle`
- :py:class:`ambry.bundle.loader.TsvBuildBundle`
- :py:class:`ambry.bundle.loader.ExcelBuildBundle`
- :py:class:`ambry.bundle.loader.GeoBuildBundle`

The next step is to configure the :ref:`Loader with sources and start creating metadata. <using_loaders>`

Sources
*******

The Loader classes work off of files defined in the  ``sources`` metadata, stored in the file :file:`meta/build.yaml`. ``Sources`` is a dictionary, so the first level is the key for each entry, then the keys of the entry. A new bundle should have one named ``example``. 



