---
layout: default
title: 'Building Ambry Bundles'
---

# Configuration

Python dependencies. 

You can configure bundle dependencies in the bundle.yaml file. They will be installed with pip. 

    build:
        requirements:
            bs4: beautifulsoup4
            dbfpy: http://downloads.sourceforge.net/project/dbfpy/dbfpy/2.2.5/dbfpy-2.2.5.tar.gz?r=http%3A%2F%2Fsourceforge.net%2Fprojects%2Fdbfpy%2Ffiles%2F&ts=1384979899&use_mirror=hivelocity

In the requirements dict, the key is the name of the module to import, and the value is the URL or import name. Most often, they are the same, but they can be different, as in the case of beautifulsoup, where the import and the cheeseshop package have different names, or in the case of dbfoy, which is not in the cheeseshop and must be download from a url. 

This feature requires the filesystems.python value to be set to a directory where the modules will be downloaded, installed, and included from. 


