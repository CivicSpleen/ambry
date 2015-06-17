.. _common_challenges:


Common Challenges
=================

Refer to the snippets of source and configuration below for solutions for common bundle challenges. 

Complex Source Files
********************

* `Complex Source Files`_
* `Multiple sheets in an Excel file`_
* `Nulls or other illegal characters`_
* `Character Encoding`_


Multiple files in a zip file
----------------------------

Very often there will be multiple source files in a single zip file. You can select a specific file in the zip file with a regular expression in the source's ``file`` value. The value need only be a unique subset of the file name you want to select, not necessarily a regular expression. 

.. code-block:: yaml

    hhcahps_national:
        description: National averages for the patient experience of care survey measures.
        file: HHC_SOCRATA_HHCAHPS_NATIONAL.csv
        url: http://data.medicare.gov/views/bg9k-emty/files/K2mijv-Kwa3BxIvmpxh3ZYiFHcn_15Cd4WbvhBb9m3s?filename=HHCompare_Revised_FlatFiles.zip
    

Multiple sheets in an Excel file
--------------------------------

When importing an Excel spreadsheet, it is common for the file to have multiple worksheets. You can select a specific sheet with the soruce's ``segment`` value: 

.. code-block:: yaml

    2012-2013_7th:
        description: Seventh grade immunizations for 2012
        row_spec:
            data_end_line: 3854
            data_start_line: 5
            header_comment_lines:
            - 0
            - 1
            header_lines:
            - 3
            - 4
        segment: 3
        table: g7
        time: 2012
        url: http://www.cdph.ca.gov/programs/immunize/Documents/2012-2013%20CA%20Seventh%20Grade%20Data.xls

Nulls or other illegal characters
---------------------------------

If a source file has a fomatting error that a row generator class considers illegal, such as embedded nulls, you can intercept the file and clean it before it enters the row generator by overriding the :meth:`line_mangler` method in a Loader bundle: 

.. code-block:: python

    from ambry.bundle.loader import CsvBundle

    class Bundle(CsvBundle):
    
        def line_mangler(self, source, l):

            return l.replace('\0', '')


Character Encoding 
------------------

Many datasets are encoded in a non-ascii encoding, sometimes using characters that cause the :class:`DelimitedRowGenerator` row generator to choke. If so, you can explicitly set the encoding for a source with the ``encoding`` value.

.. code-block:: yaml

    hhcahps_prvdr:
        description: Information on the Patient Experience of Care Survey results
            for each home health agency.
        encoding: latin-1
        file: HHC_SOCRATA_HHCAHPS_PRVDR.csv
        url: http://data.medicare.gov/views/bg9k-emty/files/K2mijv-Kwa3BxIvmpxh3ZYiFHcn_15Cd4WbvhBb9m3s?filename=HHCompare_Revised_FlatFiles.zip

    


