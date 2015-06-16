.. _bundle_configuration:

Configure a Bundle
==================

Configuring a bundle for building involves setting the metadata, sources, dependencies and requirements, and for many basic datasets, selecting the right Bundle class. 

Basic Metadata
**************

Open the :file:`bundle.yaml` file to edit the basic metadata. 

The most important values to set are: 

- about.access
- about.title
- about.summary
- external_documentation.dataset

First, find the main web page for the dataset. This should be a page that has the title of the dataset and some basic overview information about it, the first page you are likely to find, and a page that links to other pages about the dataset. Describe this page and set its URL in the ``external_documentation.dataset`` variable. If there is a seperate page with links to downloadable files, set this URL in ``external_documentation.download``. If there is one page that both describes the dataset and has links to files, set it in ``external_documentation.dataset``

If there is a seperate page or PDF for the dataset documentation, set it in ``external_documentation.documentation``, but remove the section if this information does not exist, or already exists on the ``external_documentation.dataset`` page. 

The Title and Summary can usually be copied directly from the main dataset page, but if not, they should be written carefully, as they are used to describe the dataset thoughout the dataset's documentation. 

You will need to have ``about.access`` set to build the bundle, since it is required to determine which remote library should store the bundle. For this demo, set it to 'example'.

Fill in other  ``about`` entries as appropriate: 

- about.license: 'public' if there is no declared license, otherwise the name or, better a URL, to the license. 
- about.processed: A statement about how the data were processed, if there were substantial changes from the original sources
- about.footnote: A statement about unusual interpretations or meaning of the dataset, or anything else that would be approprate in a footnote. 
- about.time: A year or year range that the dataset covers. Seperate start and end years with a '/'
- about.space: The name of the state or county that the data cover, or "US" for national data. Must be a term that returns a hit from the search function :command:`ambry search -I`
- about.access: A name of a remote library where the built bundle will be uploaded. Usually "public"

For the USDA example, here are the parts of the :file:`bundle.yaml` that are changed from the default values: 

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


Other Documentation
*******************

While most of the documentation for a dataset is provided by the dataset source, there is also documentation that the bundle author can add to the bundle.

The :file:`meta/documentation.md` is the place to put information about the dataset that is not covered in an external document, such as errors covered in conversion, modificatons made to the dataset in conversion, or information that was provided by the source in email, for which there is no URL. 


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





