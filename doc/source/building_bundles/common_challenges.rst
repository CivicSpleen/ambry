.. _common_challenges:


Common Challenges and Errors
============================


Refer to the snippets of source and configuration below for solutions for common bundle challenges. 

Complex Source Files
********************

* `Multiple files in a zip file`_
* `Multiple sheets in an Excel file`_
* `Nulls or other illegal characters`_
* `Character Encoding`_
* `Tabs or Commas or Excel? Forcing a file type`_
* `ZIP Urls without a ZIP Extension`_


Multiple files in a zip file
----------------------------

Very often there will be multiple source files in a single zip file. You can select a specific file in the zip file with a regular expression in the source's ``file`` value. The value need only be a unique subset of the file name you want to select, not necessarily a regular expression. 

.. code-block:: yaml
    :emphasize-lines: 3

    hhcahps_national:
        description: National averages for the patient experience of care survey measures.
        file: HHC_SOCRATA_HHCAHPS_NATIONAL.csv
        url: http://data.medicare.gov/views/bg9k-emty/files/K2mijv-Kwa3BxIvmpxh3ZYiFHcn_15Cd4WbvhBb9m3s?filename=HHCompare_Revised_FlatFiles.zip
    

Multiple sheets in an Excel file
--------------------------------

When importing an Excel spreadsheet, it is common for the file to have multiple worksheets. You can select a specific sheet with the soruce's ``segment`` value: 

.. code-block:: yaml
    :emphasize-lines: 12

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

.. versionadded:: 0.3.953

If a source file has a fomatting error that a row generator class considers illegal, such as embedded nulls, you can intercept the file and clean it before it enters the row generator by overriding the :meth:`line_mangler` method in a Loader bundle: 

.. code-block:: python
    :emphasize-lines: 5

    from ambry.bundle.loader import CsvBundle

    class Bundle(CsvBundle):
    
        def line_mangler(self, source, row_gen, l):

            return l.replace('\0', '')


Character Encoding 
------------------

.. versionadded:: 0.3.953

Many datasets are encoded in a non-ascii encoding, sometimes using characters that cause the :class:`DelimitedRowGenerator` row generator to choke. If so, you can explicitly set the encoding for a source with the ``encoding`` value.

.. code-block:: yaml
    :emphasize-lines: 4

    hhcahps_prvdr:
        description: Information on the Patient Experience of Care Survey results
            for each home health agency.
        encoding: latin-1
        file: HHC_SOCRATA_HHCAHPS_PRVDR.csv
        url: http://data.medicare.gov/views/bg9k-emty/.../filename=HHCompare_Revised_FlatFiles.zip

The value of `encoding` can be any valid Python encoding. If the encoding is `ascii` or `unknown`, the Row Generator will use the builtin python csv module, rather than unicodecsv. 

    
Tabs or Commas or Excel? Forcing a file type
--------------------------------------------

.. versionadded:: 0.3.953

The row generator will automatically select the comma character for a field delimiter if the extension of the file is ``.csv``, or a tab ( `\\t` ) if the extension is ``.tsv``, or excel for ``.xls``. If the file does not have a file extension that properly triggers the right behavior in the row generator, you can force the file type with the ``filetype`` value in the source.

Here is an example where the file has a ``.txt`` extension, but is actually a CSV file. 

.. code-block:: yaml
    :emphasize-lines: 4

    puf_10_northb:
        description: Public Discharge Data, Public Use File 2010
        file: Northb_lbl.txt
        filetype: csv
        table: pdd_puf
        time: 2010
        url: s3://.../.../Public10.zip

ZIP Urls without a ZIP Extension
--------------------------------

.. versionadded:: 0.3.956

If a URL for a zip file doesn't end with `.zip`it may not be unzipped properly. You can force  the URL to be interpreted with `.zip` with the `urlfiletype` value

.. code-block:: yaml
    :emphasize-lines: 6

    national:
        description: National downloadable file
        encoding: ascii
        file: National_Downloadable_File
        filetype: csv
        urlfiletype: zip
        url: https://data.medicare.gov/views/bg9k-emty/files/mCHhGYGNCpKUrsgtlt7YLwUxGS-LYOYkJBzBM7uzKlM?filename=Physician_Compare_Databases.zip&content_type=application%2Fzip%3B%20charset%3Dbinary
    

Exceptions
**********

Exception: Unknown source file extension
----------------------------------------

While running ``meta_set_row_specs`` or the ``meta`` phase, this error means that the file extension for the source file is unknown. It can be set with the ``source`` value ``filetype``, with valid values being: ``csv``, ``tsv`` and ``xls``

.. code-block:: yaml
    :emphasize-lines: 6

    sources:
        national:
            description: National downloadable file
            encoding: latin-1
            file: National_Downloadable_File
            filetype: csv           
            url: https://data.medicare.gov/views/bg9k-emty/.../charset%3Dbinary
    

UnicodeDecodeError: 'utf8' codec can't decode
---------------------------------------------

While reading files, this error occurs when the Unicode CSV reader encounters  file encoding that isn't ASCII or UTF8. You can explicityly set the file encoding on a source with the  ``encoding`` value, where the value is any valid name for an encoding in Python. The most common is probably ``latin-1``

.. code-block:: yaml
   :emphasize-lines: 4
   
    sources:
        national:
            description: National downloadable file
            encoding: latin-1
            file: National_Downloadable_File
            filetype: csv 
            url: https://data.medicare.gov/views/bg9k-emty/.../charset%3Dbinary
    

