#!/bin/bash

is_dev=$1

echo "--- Installing Ambry"
apt-get update
locale-gen en_US.UTF-8

packages="git gcc g++ python-pip python-dev sqlite3  libpq-dev
libgdal-dev gdal-bin python-gdal python-numpy python-scipy "
#libhdf5-serial-dev libhdf5-dev hdf5-tools h5utils  python-h5py \
# "

let "ver = $(lsb_release -r -s | tr -d '.')"

if (( $ver >= 1404 )); then
    packages="$packages libsqlite3-dev libspatialite3 libspatialite-dev spatialite-bin libspatialindex-dev"
else
    packages="$packages libsqlite3-dev libspatialite5 libspatialite-dev spatialite-bin libspatialindex-dev"
fi




for pkg in $packages; do
    echo "INSTALLING: $pkg"
    apt-get install -y $pkg
    if [ $? != 0 ]; then
        echo "ERROR: Failed to install $pkg"
        exit $?
    fi
done

# This package allows Sqlalchemy to load the spatialite shared object to provide
# Spatialite services.
pip install git+https://github.com/clarinova/pysqlite.git#egg=pysqlite

###
### Install Ambry
###

if [ ! -z "$is_dev" ]; then
    pip install -e "git+https://github.com/clarinova/ambry.git#egg=ambry"
else
    pip install ambry
fi

ambry config install # Installs a development config

cd $(ambry config value filesystem.source)

[ ! -e clarinova-public ] && git clone https://github.com/clarinova/ambry-bundles-public.git clarinova-public
