Configuring Sources
===================

Topics

- Basic Ingestion
- Source parameters. 
- Fixed Width
- Excel
- Complex headers
- Types of sources

    - File soruces
    - Generator soruces
    - SQL Sources


Ingestion load the exernal source files into local files in the build directory. The local build files are 
a special format for row data that are easier to read and process than the wide collection of source files. 

First, set up the sources.csv file

bambry ingest


Source Configuration
********************

- name. Name of the source record. Required, and must be unique. 
- title. Optional title. Used as the title of the destination table. 
- source_table. Optional source table name. If not supplied, the name of the source is assumed. 
- dest_table. Optional destination table name. If not supplied, the name of the source is assumed. 
- segment. For Excel files, the number of the spreadsheet to use within the workbook. 
- file. For ZIP files, a regualr expression specifying the file to extract
- filetype. For files that don't have an informative extension this value gives the assumed extension, such as 'tsv','csv', or 'xls'
- encoding. The file encoding name, such as 'latin1' or 'uft8'. UTF8 is assumed. 
- time. A year or other time specifier to distinguish the source. May be used in the partition name. 
- space. A geo region name, like a county or state. May be used in the partition name. 
- grain. An aggregation grain. May be used in the partition name. 
- start_line. The integer, zero-indexed row that the is first row with data. 
- end_line. The intger, zero-index row that is the last row with data. 
- comment_lines. A List of zero-index rows that are comments. 
- header_lines. A List of zero-index row numbers that are headers. These rows will be coalesced into column names. 
- description. An optional description
- reftype. A type code for the value in `ref`. The value is unconstrained, but these values have special meaning: 'ref', 'partition', 'generator', 'sql', 'template'
- ref. A url or code object name. 

Primary References
------------------

- reftype. A type code for the value in `ref`. The value is unconstrained, but these values have special meaning: 'ref', 'partition', 'generator', 'sql', 'template'
- ref. A url or code object name. 


Table, Time, Space and Grain
----------------------------

These values specify the four common categorization dimension, which are part of every bundle and partition name. 

- dest_table. Optional destination table name. If not supplied, the name of the source is assumed. 
- time. A year or other time specifier to distinguish the source. May be used in the partition name. 
- space. A geo region name, like a county or state. May be used in the partition name. 
- grain. An aggregation grain. May be used in the partition name. 

Row Classification
------------------

Four values indicate the classification of rows in the source file: 

- start_line. The integer, zero-indexed row that the is first row with data. 
- end_line. The intger, zero-index row that is the last row with data. 
- comment_lines. A List of zero-index rows that are comments. 
- header_lines. A List of zero-index row numbers that are headers. These rows will be coalesced into column names. 

Multi-file archives: Zip and Excel
----------------------------------

Some types of sources have multiple files in them, such as Excel files, with multiple worksheets in a workbook, or a ZIP archive. 

- segment. For Excel files, the number of the spreadsheet to use within the workbook. 
- file. For ZIP files, a regualr expression specifying the file to extract

Fixed Width Files
-----------------

For fixed with files, create a source schema that has the column names for the file, including colum positions and widths. 

Column Names From Headers
*************************

When a source file has a header row, the row names are namgled to create column names. Mangling involves: 

- Convert Non-alphanumeric characters to underscores
- Lowercasing
- Stripping leading and trailing underscores

The code is: 

.. code-block:: python 

    return re.sub('_+', '_', re.sub('[^\w_]', '_', name).lower()).rstrip('_')

If the file doesn't have a header -- the `start_line` is 0 -- the columns are named `col<n>` for column in position `<n>`.

If there are multiple row numbers in the `header_lines` value, the column names are created by:

- For each row, if a column is blank, ( space or null) copy into it the value to the left. ( Column copy-forward)
- When all header rows have values, concatenate all of the rows by column into a single concatenated row. 
- Apply name mangling to all of the columns. 

Note that the column copy-forward only applies to the first header row.

For instance, if the source file header has this structure: 

==  ==  ==  ==  ==
a       b       c
1!  2#  3$  4%  5&
1   2   3   4   5
==  ==  ==  ==  ==

The column copy operation would transform it to: 

==  ==  ==  ==  ==
a   a   b   b   c
1!  2#  3$  4%  5&
1   2   3   4   5
==  ==  ==  ==  ==

After coalescing the result is: 

======  ======  ======  ======  ======
a 1! 1  a 2# 2  b 3$ 3  b 4% 4  c 5& 5
======  ======  ======  ======  ======

And after name mangling: 

=====  ===  =====  ===  =====
1_a_1  2_2  3_b_3  4_4  5_c_5
=====  ===  =====  ===  =====



Ingesting
*********

Topics

- Ingesting single sources
- Ingesting Tables
- Ingest everything

Row and Type Intuition
**********************

When a source is ingested two Intuiters are run, the Type Intuiter and the Row Intuiter.  The Type Intuiter tries to determine the datatype for each column. 


Viewing Ingested Files
**********************

Topics

- Basic info
- Schema
- Types


Types of Sources
****************


File Sources
------------


Generator Sources
-----------------


SQL Sources
-----------



Notes
*****

Ingesting excel files with dates can be troublesome.

If you change the sources.csv. 