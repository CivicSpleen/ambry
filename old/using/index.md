---
layout: sidebar
title: 'Using Ambry'
---

# Using Ambry

## Getting Data With Ambry

* List the library
* Get a bundle and partition
* Use the partition for analysis
* Install to the database
* Install to a data repository

### List the Library

Ambry maintains a local library that is synchonized with a directory that holds bundle files and possible with one or more remote libraries. Using the `list` command, we can search for files that contain "crime" in the name. 

{% highlight bash %}
$ ambry list crime
   R  d00q007        clarinova.com-crime-incidents-casnd-0.1.7
 S R  d00q008        clarinova.com-crime-incidents-casnd-geocoded-0.1.8
   R  d02j002        clarinova.com-crime-incidents-casnd-linked-0.1.2
 S    d02j003        clarinova.com-crime-incidents-casnd-linked-0.1.3
 S    d00B001        clarinova.com-us_crime_incidents-state-0.1.1
   R  d00L002        sandag.org-crimeincidents-orig-0.1.2    
 S    d00L003        sandag.org-crimeincidents-orig-1.0.3    
 S    dtdUWPHq0f001  sandiegodata.org-crime-test-casnd-0.0.1 
{% endhighlight %}

The listing has three important sections. The first indicates where the data package exists. The code "S" means it is a source package, in the `/data/source`. The "R" code means that the package is available at the remote, in this case, at `http://library.clarinova.com`.

The next column is the Object Number, which uniquely identifies each data bundle. The last column is the versioned name. The last three digits of the Object Number is the revision number, which is the same as the last digit of the version number in the name. 

When referencing a bundle, you can use either the Object Number or the name, and sometimes, you can use the number or the name without the version number, if you want the latest version. 

Next, lets look at the information about one of the bundles, a crime set. 

{% highlight bash %}
$ ambry info -p clarinova.com-crime-incidents-casnd-0.1.7
D --- Dataset ---
D Vid       : d00q007
D Vname     : clarinova.com-crime-incidents-casnd-0.1.7
D Fqname    : clarinova.com-crime-incidents-casnd-0.1.7~d00q007
D Locations :    R  
D Rel Path  : clarinova.com/crime-incidents-casnd-0.1.7.db
D Abs Path  : 
B Source Dir: None
D Partitions: 1
P p00q001007      clarinova.com-crime-incidents-casnd-incidents-0.1.7
{% endhighlight %}

This listing tells us that the bundle has not been downloaded yet, and it has one data partition. We'll use that data partition in our analysis. 

### Get a Partition

To get a partition from the remote library, use the `library get` command. This command will run for a while while it downloads, then return information about the partition. 

{% highlight bash %}
$ ambry library get clarinova.com-crime-incidents-casnd-incidents-0.1.7
D --- Dataset ---
D Vid       : d00q007
D Vname     : clarinova.com-crime-incidents-casnd-0.1.7
D Fqname    : clarinova.com-crime-incidents-casnd-0.1.7~d00q007
D Locations :   LR  
D Rel Path  : clarinova.com/crime-incidents-casnd-0.1.7.db
D Abs Path  : /data/library/clarinova.com/crime-incidents-casnd-0.1.7.db
B Source Dir: None
P --- Partition ---
P Partition : p00q001007; clarinova.com-crime-incidents-casnd-incidents-0.1.7
P Is Local  : True
P Rel Path  : clarinova.com/crime-incidents-casnd-0.1.7/incidents.db
P Abs Path  : /data/library/clarinova.com/crime-incidents-casnd-0.1.7/incidents.db
$ 

{% endhighlight %}

The locations line now reads 'LR' to indicate that the bundle stored locally, and we have an Abs Path for both the bundle metadata and the partition. The path is to a Sqlite file, which you can open and use directly, or can access through other methods. 

### Use the Partition for Analysis

To use the data in the partition, you'll probably want to look at the schema. You can dump the schema in a variety of formats with the `library schema` command, which produces CSV by default: 

{% highlight bash %}
$ ambry library schema p00q001007 | head -5
table,seq,column,is_pk,is_fk,type,i1,i10,i11,i2,i3,i4,i5,i6,i7,i8,i9,size,default,description,sql,id
incidents,1,incidents_id,1,,INTEGER,,,,,,,,,,,,,,,,c00q01001
incidents,2,type,,,VARCHAR,1,,,,,,,,,,,10,-,"Arrest, Citation or Crime Case",,c00q01002
incidents,3,agency,,,VARCHAR,,,,,,,,,,,,30,-,Police agency that originated the incident,,c00q01003
incidents,4,datetime,,,DATETIME,,,,1,,,,,,,,,01/01/00,Date and time in ISO format,,c00q01004
{% endhighlight %}

Using the schema, you can open the partition and run SQL commands on it: 

{% highlight bash %}
$ sqlite3 /data/library/clarinova.com/crime-incidents-casnd-0.1.7/incidents.db \
    "SELECT DISTINCT legend FROM incidents LIMIT 5"
-
ARSON
ASSAULT
BURGLARY
DRUGS/ALCOHOL VIOLATIONS
{% endhighlight %}

You can also access the partition using the Python library. 

{% highlight python  %}
import ambry
import pandas as pd
l = ambry.ilibrary()

p = l.get('clarinova.com-crime-incidents-casnd-incidents-0.1.7').partition

for row in p.query("SELECT DISTINCT legend FROM incidents LIMIT 5"):
    print row
{% endhighlight %}


See the IPython note book [Introduction to Ambry](/notebooks/tutorial/Ambry_Introduction.html) for a more detailed example. 


### Install to the Database

Bundles can be installed to relational databases. 

### Install to a Data Repository

Bundle can also be installed to a data repository, such as the repository for the [San Diego Regional Data Library](http://data.sandiegodata.org). 

## Building Packages


Naturally, before you canfind, install and use a data bundle, someone had to create it. Bundles are composed of two files, one with Python language code, and the other is a YAML configuration file.  

Creating a bundle involes: 


* Create a Package Directory
* Write the Bundle Class and Configuration
* Build The Bundle
* Submit to the library. 

### Create a Package Directory

The `source new` command creates bundle directories, with skeletons of the required files.



{% highlight bash %}
$ ambry source new -s example.com -d demo 
Got number from number server: d000001F
Sync source bundle: /data/source/foo/example.com-demo 
CREATED: example.com-demo-0.0.1~d000001F001, /data/source/foo/example.com-demo

{% endhighlight %}

The resulting director contains all of the required files, and skeleton code, so it can be built immediately. 

{% highlight bash %}
$ cd example.com-demo
$ ls -la
total 32
drwxr-xr-x  8 eric  staff   272 May 15 08:34 .
drwxr-xr-x  3 eric  staff   102 May 15 08:34 ..
-rw-r--r--  1 eric  staff   219 May 15 08:34 README.md
drwxr-xr-x  3 eric  staff   102 May 15 08:34 build
-rw-r--r--  1 eric  staff   789 May 15 08:34 bundle.py
-rw-r--r--  1 eric  staff  1400 May 15 08:34 bundle.pyc
-rw-r--r--  1 eric  staff   446 May 15 08:34 bundle.yaml
drwxr-xr-x  3 eric  staff   102 May 15 08:34 meta
{% endhighlight %}

{% highlight bash %}
$ bambry build
{% endhighlight %}

Note that to interact with the bundle, we're using the `bambry` command, which is a synonym for `ambry bundle -d $(pwd)`, a short cut for using the `ambry` program. 

### Write the Bundle Class and Configuration

{% highlight bash %}
{% endhighlight %}

### Build The Bundle

### Submit to the library. 