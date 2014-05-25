#!/bin/bash

echo "--- Installing base packages. May need to ask for root password"
sudo apt-get update

packages=" git gcc g++ python-pip python-dev sqlite3  libpq-dev
libsqlite3-dev libspatialite3 libspatialite-dev spatialite-bin "
#libgdal-dev gdal-bin python-gdalpython-numpy python-scipy \
#libhdf5-serial-dev libhdf5-dev hdf5-tools h5utils  python-h5py \
#libspatialindex-dev "

for pkg in $packages; do
    echo "INSTALLING: $pkg"
    sudo apt-get install -y $pkg
    if [ $? != 0 ]; then
        echo "ERROR: Failed to install $pkg"
        exit $?
    fi
done

# This package allows Sqlalchemy to load the spatialite shared object to provide
# Spatialite services.
sudo pip install git+https://github.com/clarinova/pysqlite.git#egg=pysqlite

###
### Install Ambry
###

sudo mkdir -p /data/src
user=$(whoami)

cd /data/

sudo pip install git+https://github.com/clarinova/ambry.git#egg=ambry

# Install the example sources
mkdir /data/source

cd /data/source
git clone https://github.com/sdrdl/sdrdl-ambry-bundles.git sdrdl
git clone https://github.com/clarinova/ambry-bundles-public.git clarinova-public

sudo chown -R $user /data

ambry config install # Installs a development config