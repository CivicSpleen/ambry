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

- about.access
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

Here are the parts of the :file:`bundle.yaml` that are changed from the default values for creating the USDA agricultural productivity example bundle: 

.. code-block:: yaml

    about:
        access: example
        rights: public
        space: US
        summary: This data set provides estimates of productivity growth in the U.S. farm sector 
            for the 1948-2011 period, and estimates of the growth and relative levels of productivity
            across the States for the period 1960-2004.
        time: 1948/2011
        title: Agricultural Productivity in the U.S.

    external_documentation:
        dataset:
            description: Overview of the dataset, with links to the data files. 
            title: Overview and Download Page
            url: http://www.ers.usda.gov/data-products/agricultural-productivity-in-the-us.aspx


Source and Documentation Links
******************************

Once you've setup the basic metadata, and in particular, set the download and/or dataset values, you can set the source URLS. These are references to the source files that will be loaded into the bundle. 

The easiest way to get these links is to run :command:`bambry config scrape`. This will extract the links from the pages specified by ``external_documentation.download`` and ``external_documentation.dataset``, looking for PDF, CSV and XLS files. It will dump the links in the proper formats for the ``sources`` and ``external_documentation`` sections. XLS and CSV files will go in the sources section, while PDF files will go in the external_documentation section. 

You can often just copy these into the configuration. The sources go into the ``sources`` section in the :file:`meta/build.yaml` file. You can also copy in the exteral_documentation values, but it's usually better to only copy the most important ones, since users will usually prefer to use the links from the original page, rather than from the Ambry documentation. 

For the USDA agricultural productivity example bundle, :command:`bambry config scrape` finds about 25 data links, most of which are named 'table' with a number, and have no description. It would be best to change the change to be more informative, but we'll do that later. For now, here is what the  ``sources`` section in :file:`meta/build.yaml` looks like: 


.. code-block:: yaml

    sources:
      StatePriceIndicesAndQ:
        description: None
        url: /dataFiles/Agricultural_Productivity_in_the_US/StateLevel_Tables_Price_Indicies_and_Implicit_Quantities/StatePriceIndicesAndQ.xls
      table01:
        description: none
        url: http://www.ers.usda.gov/datafiles/Agricultural_Productivity_in_the_US/National_Tables/table01.xls
      table02:
        description: None
        url: http://www.ers.usda.gov/datafiles/Agricultural_Productivity_in_the_US/National_Tables/table02.xls
      table03:
        description: None
        url: http://www.ers.usda.gov/datafiles/Agricultural_Productivity_in_the_US/StateLevel_Tables_Relative_Level_Indices_and_Growth_19602004Outputs/table03.xls
      table04:
        description: None
        url: http://www.ers.usda.gov/datafiles/Agricultural_Productivity_in_the_US/StateLevel_Tables_Relative_Level_Indices_and_Growth_19602004Outputs/table04.xls

Note that for this example, the :command:`bambry config scrape` reported all of the URLs as relative, starting with `/dataset`. The URLs were manually edited to add the schema and host. 


Select Bundle Class
*******************

If you will be building from one or more CSV, Excel or TSV files, edit the :file:`bundle.py` to change the base class to one from :py:mod:`ambry.bundle.loader`.

- :py:class:`ambry.bundle.loader.CsvBundle`
- :py:class:`ambry.bundle.loader.TsvBuildBundle`
- :py:class:`ambry.bundle.loader.ExcelBuildBundle`
- :py:class:`ambry.bundle.loader.GeoBuildBundle`

For the USDA agricultural productivity example bundle, since  all of the fiels are Excel format, we'll use the :py:class:`ambry.bundle.loader.ExcelBuildBundle` class. The result is a nearly empty :file:`bundle.py` file

.. code-block:: python 

    from ambry.bundle.loader import ExcelBuildBundle

    class Bundle(ExcelBuildBundle):

        pass


The next step is to configure the :ref:`Loader with sources and start creating metadata. <configuring_sources>`





