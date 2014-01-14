---
layout: sidebar
title: 'Ambry: Comprehensive Data Package Management'
---

# Naming and Numbering

## Object Numbering

Ambry uses structured Base-62 strings to uniquely identify objects in the system. These numbers frequently appear as prefixes to table names and similar places, so it is important to keep them short; a typical UUID would have been too long. Because the numbers are short, there is a more limited space for allocation, which requires a centralized number server, although there is an alternative that allows users to generate longer numbers without the central authority. 

The objects that Ambry enumerates are: 

  * Bundles, also called Datasets. 
  * Partitions, a part of a Bundle
  * Tables, a part of a Bundle
  * Columns, a part of a Table. 
  
Because all of these objects are related, the partitions, Tables and Columns all have numbers that are based on the number of the BUndle the object is part of. 

All of the numbers are integers expressed in Base-62, which uses only digits and numbers. 
  
  bdigit            = ALPHA / DIGIT
  
  bundle_seq        = ( 5bdigit / 9bdigit )
  
  bundle_number     = "d" bundle_seq
  
  partition_seq     = 3bdigit 
  
  partition_number  = "p" bundle_seq partition_seq 
  
  table_seq         = 2bdigit
  
  table_number      = "t" bundle_seq table_seq
  
  column_seq        = 3bdigit
  
  column_number     = "c" bundle_seq table_seq column_seq
  
  revision          = 3bdigit
  
  object_number     = ( bundle_number / partition_number / column_number 
                      table_number ) [revision]
                      

There are two lengths for the bundle sequence: 5 digits or 9 digits. The 5 digit numbers are assigned by a central authority, so the number space is dense. ( 5 Base-62 digits is approximately 1 billion numbers. ) The 9 digit numbers are self assigned and are chosen randomly. 

All bundles have a revision, and the bundle's revision number is used for all of the Bundle's objects. However, the revision is optional in many cases, such as when referencing an object with respect to a specific bundle, or when only one version of a bundle is in stalled in a database. 

Because of these variations, object numbers can have a variety of lengths. BUndles, for instance, can have 1 + ( 5 | 9) + ( 0 | 3) = 6, 9, 10, 13 characters. 





