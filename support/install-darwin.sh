#!/bin/bash 

echo "--- Installing base packages. May need to ask for root password"

sudo echo

#
# Install scikit, scipy, numpy and others, on Mac OS X
#  curl -o install_superpack.sh https://raw.github.com/fonnesbeck/ScipySuperpack/master/install_superpack.sh
#  sh install_superpack.sh
#

command -v brew >/dev/null 2>&1

if [ $? -ne 0 ]; then
    echo
    echo "ERROR: This script requires the brew package manager "

    echo "Press y to download and run brew installation"
    read -n 1 yn
    if [ "$yn" == 'y' ]; then
        ruby -e "$(curl -fsSL https://raw.github.com/mxcl/homebrew/go/install)"
    else
        exit 1
    fi

fi

# Upgrade setuptools
#curl  https://bitbucket.org/pypa/setuptools/raw/bootstrap/ez_setup.py | sudo python

which clang > /dev/null

if [ $? -ne 0 ]; then
    echo
    echo "ERROR: First, install XCode and the command line tools to get the C compiler. "
    exit 1	
fi	

##
## Install packages with brew that are required to build python packages.
##
# To deal with recent changes in clang.
export ARCHFLAGS="-Wno-error=unused-command-line-argument-hard-error-in-future"

echo "--- Installing packages with Homebrew"

brew_packages="git gdal spatialite-tools postgresql homebrew/science/hdf5 spatialindex "

for pkg in $brew_packages; do
    brew install $pkg
    if [ $? -ne 0 ]; then
	    echo "ERROR: brew package did not install: " $pkg
	    exit 1
    fi
done


##
## Install the python requirements
##

sudo easy_install pip

sudo ARCHFLAGS="-Wno-error=unused-command-line-argument-hard-error-in-future" \
pip install -r https://raw.githubusercontent.com/clarinova/ambry/master/requirements.txt


##
## Check that gdal was installed correctly, and refer user to KyngChaos if not.
##

gdal_version=$(python -c 'import gdal; print gdal.VersionInfo()')

if [ $? -ne 0 ]; then
    echo
    echo "ERROR: GDAL not found. Install the KyngChaos \"GDAL Complete\" framework, from http://www.kyngchaos.com/software/frameworks#gdal_complete"
    echo "Press y to visit the GDAL download Page, or any other key to cancel"
    read -n 1 yn
    if [ "$yn" == 'y' ]; then
        open 'http://www.kyngchaos.com/software/frameworks#gdal_complete'
        echo "\nBe sure to install both GDAL Complete and Numpy"
        exit 0
    else
        exit 1
    fi
fi	

if [ $gdal_version -lt 1920 ]; then
    echo
    echo "ERROR: GDAL Found, but version $gdal_version is too old. Upgrade with KyngChaos frame work, "
    echo "Press y to visit the GDAL download Page, or any other key to cancel"
    read -n 1 yn
    if [ "$yn"  == 'y' ]; then
        open 'http://www.kyngchaos.com/software/frameworks#gdal_complete'
        exit 0
    else
        exit 1
    fi
fi

##
## Actually install Ambry
##

sudo easy_install pip

sudo ARCHFLAGS="-Wno-error=unused-command-line-argument-hard-error-in-future" \
pip install -r https://raw.githubusercontent.com/clarinova/ambry/master/requirements.txt

sudo mkdir -p /data/src
sudo mkdir -p /data/source

user=$(whoami)

cd /data/src

sudo pip install -e git+https://github.com/clarinova/ambry.git#egg=ambry

sudo chown -R $user /data

##
## Configure ambry
##

ambry config install




