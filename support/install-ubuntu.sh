#!/bin/bash

echo "--- Installing base packages. May need to ask for root password"
sudo apt-get update

packages=" \
git gcc g++ \
python-pip python-dev \
sqlite3  libsqlite3-dev libspatialite3 libspatialite-dev spatialite-bin \
libpq-dev \
libgdal-dev gdal-bin python-gdal python-h5py python-numpy python-scipy \
libhdf5-serial-dev libhdf5-dev hdf5-tools h5utils \
libspatialindex-dev "

for pkg in $packages; do
    echo "INSTALLING: $pkg"
    sudo apt-get install -y $pkg
    if [ $? != 0 ]; then
        echo "ERROR: Failed to install $pkg"
        exit $?
    fi
done


sudo pip install git+https://github.com/clarinova/pysqlite.git#egg=pysqlite

sudo pip install -r https://raw.githubusercontent.com/clarinova/ambry/master/requirements.txt

sudo mkdir -p /data/src
user=$(whoami)

cd /data/

sudo pip install -e git+https://github.com/clarinova/ambry.git#egg=ambry

sudo chown -R $user /data