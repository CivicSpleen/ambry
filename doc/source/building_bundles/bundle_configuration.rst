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

The Title and Summary can usually be coppied directlry from the ``external_documentation.dataset`` page, but if not, they should be written carefully, as they are used to describe the dataset thoughout the dataset's documentation. 

Source and Documentation Links
******************************

Once you've setup the basic metadata, and in particular, set the download or documentation values, you can set the source URLS. These are references to the source files that will be loaded into the bundle. 

The easiest way to get these links is to run :command:`bambry config scrape`. This will extract the links from the pages specified by ``external_documentation.download`` and ``external_documentation.dataset``, looking for PDF, CSV and XLS files. It will dump the links in the proper formats for the ``sources`` and ``external_documentation`` sections. XLS and CSV files will go in the sources section, while PDF files will go in the external_documentation section. 

You can often just copy these into the configuration. The sources go into the ``sources`` section in the :file:`meta/build.yaml` file. You can also copy in the exteral_documentation values, but it's usually better to only copy the most important ones, since users will usually prefer to use the links from the original page, rather than from the Ambry documentation. 


Select Bundle Class
*******************

:py:mod:`ambry.bundle.loader`

- :py:class:`ambry.bundle.loader.CsvBundle`
- :py:class:`ambry.bundle.loader.TsvBuildBundle`
- :py:class:`ambry.bundle.loader.ExcelBuildBundle`
- :py:class:`ambry.bundle.loader.GeoBuildBundle`


