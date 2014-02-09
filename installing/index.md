---
layout: default
title: 'Building Ambry Bundles'
---

# Building 


## Mac

Install XCode
You can also skip this and let Homebrew trigger the XCode download and install

Install Homebrew

Run the prepare-script

sh -c "$(curl -fsSL https://raw.github.com/clarinova/ambry/develop/support/prepare-darwin.sh)"

The Prepare script will prompt you to install other frameworks. In particularly, it will send your browser to the download page for the GDAL Complete framework. Be sure to install both GDAL Complete and Numpy.

!! These packages aren't signed, so Max OS will issue a warning. Use the right-click menu to open them with the installer. 

The installer will exit after it finds a missing dependency and directs you to the download page. After installing the sotware, run the script again. 

Sometimes the h5py install fails, with the h5py download not being found. This fixes, at a potential security problem: 

    pip install --allow-external h5py --allow-unverified h5py h5py


-----

* Install Homebrew
  * Install XCode, as part of Homebrew
* Install GDAL and Numpy, from KyngChaos 


### Troubleshooting

AttributeError: 'sqlite3.Connection' object has no attribute 'enable_load_extension'



## Linux

sh -c "$(curl -fsSL https://raw.github.com/clarinova/ambry/develop/support/prepare-ubuntu.sh)"

## Development Install



### Create the Virtual Environment

virtualenv ambry
cd ambry/
source bin/activate

pip install -e 'git+https://github.com/clarinova/ambry.git#egg=ambry'


pip install -e 'git+https://github.com/clarinova/ambry.git@develop#egg=ambry'


## Configuration

Install the configuration

ambry config install -t library \
    library.default.upstream.bucket=foobar \
    library.default.upstream.account=baz