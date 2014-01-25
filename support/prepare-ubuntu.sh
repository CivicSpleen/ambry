#!/bin/bash 

echo "--- Installing base packages. May need to ask for root password"

echo "--- Installing base packages with apt-get"
sudo apt-get install -y gdal-bin sqlite3 spatialite-bin curl git
sudo apt-get install -y python-gdal python-h5py python-numpy python-scipy
sudo apt-get install -y libpq-dev libhdf5-dev hdf5-tools h5utils
