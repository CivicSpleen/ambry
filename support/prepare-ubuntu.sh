#!/bin/bash 

echo "--- Installing base packages. May need to ask for root password"

sudo apt-get install -y curl git gcc
sudo apt-get install -y sqlite3  libsqlite3-dev libspatialite3 libspatialite-dev spatialite-bin
sudo apt-get install -y libpq-dev
sudo apt-get install -y gdal-bin  python-gdal python-h5py python-numpy python-scipy
sudo apt-get install -y libhdf5-dev hdf5-tools h5utils
