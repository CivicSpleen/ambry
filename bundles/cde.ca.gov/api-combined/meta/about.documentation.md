# {title}

Compilation of multiple years of California Academic Performance Index scores into single files. 

This dataset includes two files that combine multiple years of the [California Academic Performance Index]({website}) into single CSV files. These files have a few advantages over the original data releases from The California Department of Education:

* Multiple years of records are combined into a single file
* Files are converted to CSV format to be useful with a wider range of software tools
* Column names are normalized to be consistent across years
* Data is presented in a consistent schema, with some erroneous values removed

If you do not need to analyze multiple years and can work with fixed width text or DBF files, the original, single-year source files may be more appropriate. 

** This data is a beta release, and has not been fully verified for fidelity with the upstream source. Use with caution. **

# Differences From Sources

These files have several differences from the upstream sources:

* Multiple files, one per year, are combined into a single file.
* Yearly files have different columns, so the files in this dataset have the union of columns in each of the files. 
* Column and description names are changed to be consisten across years. 
* Non numeric field codes and flags in numeric columns are changed to negative numbers. 

## Yearly Differences in Columns

The original API data is distributed as two files per year, one for the base and one for growth targets. These seperate files have differences between them, including: 
     
* Changes to column names
* Small differences in the decriptive text
* Additions of columns in later years that were not present in earlier years. 

There are many colums that appear in only a few years, such as 'capam' or 'vnrt_m28'. Please see the files _columns_by_year_base.csv_ and _columns_by_year_growth.csv_ for reports of in which years each colum appears in the annual files. 

## Changed Column Names

Many of the columns have names or descriptions that include a year, such as 'AI_API13'. These coliumns were renamed to remove the year, and the descriptions were changed to include a phrase that indicates if the year is the current year of the file -- '13' in the 2013 file -- or the next or previous year. 

In the growth files, these column translations were made, with examples for the 2013 growth file.

<table>
    <tr><th>Year Value Description</th><th>Changed to</th><th>Example</th></tr>
    <tr><td>Current year value</td><td>_growth</td><td>api13 -> api_growth</td></tr>
    <tr><td>Previous year value</td><td>_base</td><td>api12 -> api_base</td></tr>
</table>

In the base files, There are several other substitutions. In these entries, 'YY' refers to the two digit current year, and '*' refers to one or more characters.  

<table>
    <tr><th>From</th><th>To</th></tr>
    <tr><td>apiYY</td><td>api</td></tr>
    <tr><td>apiYYb</td><td>api</td></tr>
    <tr><td>*911</td><td>*91</td></tr>
</table>

So, the columns named __api10__, __api11__, and __api12__ have all been renamed to __api__.

Column descriptions were also changed, to use a descriptive phrase for the year. Using the 2013 file as an example:

<table>
    <tr><th>Year Value Description</th><th>Example</th><th>Changed To</th></tr>
    <tr><td>Current year</td><td>2013</td><td>[This year]</td></tr>
    <tr><td>Next year</td><td>2014</td><td>[Next Year]</td></tr>
    <tr><td>Previous year</td><td>2012</td><td>[Last year]</td></tr>
    <tr><td>Last year to current year range</td><td>2012-2013</td><td>[Last year Range]</td></tr>
    <tr><td>Current year to next year range</td><td>2013-2014</td><td>[Next Year Range]</td></tr>
</table>

Three are a few other column names that we changed to be consistent across the time span: 
* 'ell' -> 'el'
* 'valid_num' ->'valid'

## Field Codes and Flags


The original source includes many columns that have numeric values with non-numeric flags to signal exceptional conditions. For instance,  the API score columns for ethnic and racial groups will include a hash mark ('#') or an asterisk ('*') when the number of students in that category is too small for statistical significance. 

In these cases, the non-numeric values have been converted to negative integers. Letter flags are converted to a negative number, where the absolute balue of the number is the orginal position of the letter in the alphabet. Other values have been converted to negative numebrs that are less than or equal to -100, and several flags are mapped to the same vaule when they have the same meaning. 

### Flags


<table>
    <tr><th>Original Code</th><th>Value</th></tr>
    <tr><td>a</td><td>-1</td></tr>
    <tr><td>b</td><td>-2</td></tr>
    <tr><td>c</td><td>-3</td></tr>
    <tr><td>d</td><td>-4</td></tr>
    <tr><td>e</td><td>-5</td></tr>
    <tr><td>f</td><td>-6</td></tr>
    <tr><td>h</td><td>-8</td></tr>
    <tr><td>i</td><td>-9</td></tr>
    <tr><td>m</td><td>-13</td></tr>
    <tr><td>n</td><td>-14</td></tr>
    <tr><td>s</td><td>-19</td></tr>
    <tr><td>t</td><td>-20</td></tr>
    <tr><td>x</td><td>-24</td></tr>
    <tr><td>y</td><td>-25</td></tr>   
</table>

These codes have meanings that differ for each column. See the descriptions in the __schema.csv__ file for the meaning of the code in a particular column. 


### Illegal and Missing Values

Codes less than or equal to -100 represent vaules that are missing, not applicable or were errors in the source files. 

<table>
    <tr><th>Orig Code</th><th>Value</th><th>Description</th></tr>
    <tr><td>#</td><td>-100</td><td>Value is not significant</td></tr>
    <tr><td>*</td><td>-100</td><td>Value is not significant</td></tr>
    <tr><td>n/a</td><td>-101</td><td>Value is missing</td></tr>
    <tr><td>n/r</td><td>-101</td><td> Value is missing</td></tr>
    <tr><td>4y</td><td>-200</td><td>An erroneous value in the source, which does not have a defined meaning. </td></tr>
    <tr><td>es</td><td>-200</td><td>An erroneous value in the source, which does not have a defined meaning. </td></tr>
</table>

### Other Changes

* All of the significance columns, which end in '_sig' have been changed to title case, so the values in this colum are NULL, 'Yes' and 'No'. Some yearly files had the values lowercased. 

# Caveats

As of version 1 of the dataset, these caveats apply: 

* _rtype_ column
    * Not used for years 200,2001 and 2002, so for these years there is no value. 
    * In years 2003 and 2004, there is a value 'A', to denote an "ASAM," Alternative Schools Accountability Model.
    * Years 2006 and later have the value 'X' to denote a summary for the state. 


# Schemas    
See the file __schema.csv__ for the names, types and descriptions of columns in the two files. 