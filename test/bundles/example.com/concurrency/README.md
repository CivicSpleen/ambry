
# Bundle README



## Notes

Partitions are organized around states and tables

## Changes to source

There are a few tables, like B24121 through B24126, that are spread across multiple segments. B24121 has about 530 columns, but the CSV files are limited to 252 columns or so. Relational databases have a hard time with a large number of columns. 

For these tables, the table is named with the Sequence Number appended. 

### Jam Codes

Jam codes are incompletely processed. Jam codes that can't be cast to an integer, such as '.', are replaced with a NULL. A future version will include a '_code' column in the tables that have Jam Codes. These new columns will hold the original value, while the data column will hold a NULL. 

There are two integer jam codes: '0' and '-1'. These values are still in the measures, so users must ensure that their queries avoid these rows when computing statistics. 
