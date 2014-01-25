#!/bin/bash 

echo "--- Installing base packages. May need to ask for root password"

#
# Install scikit, scipy, numpy and others, on Mac OS X
#  curl -o install_superpack.sh https://raw.github.com/fonnesbeck/ScipySuperpack/master/install_superpack.sh
#  sh install_superpack.sh
#

command -v brew >/dev/null 2>&1

if [ $? -ne 0 ]; then
    echo "\nERROR: This script requires the bew package manager "
    echo "Recommended to install Homebrew with: "
    echo '  ruby -e "$(curl -fsSL https://raw.github.com/mxcl/homebrew/go)"'

    echo "Press y to visit the Homebrew web page, or any other key to cancel"
    read -n 1 yn
    if [ yn == 'y' ]; then
        open 'http://brew.sh/'
        exit 0
    else
        exit 1
    fi

fi


which clang > /dev/null

if [ $? -ne 0 ]; then
    echo "\nERROR: First, install XCode and the command line tools to get the C compiler. "
    exit 1	
fi	

gdal_version=$(python -c 'import gdal; print gdal.VersionInfo()')

if [ $? -ne 0 ]; then
    echo "\nERROR: GDAL not found. Install the KyngChaos "GDAL Complete" framework, from http://www.kyngchaos.com/software/frameworks#gdal_complete"
    echo "Press y to visit the GDAL download Page, or any other key to cancel"
    read -n 1 yn
    if [ "$yn" == 'y' ]; then
        open 'http://www.kyngchaos.com/software/frameworks#gdal_complete'
        exit 0
    else
        exit 1
    fi
fi	

if [ $gdal_version -lt 1920 ]; then
    echo "\nERROR: GDAL Found, but version $gdal_version is too old. Upgrade with KyngChaos frame work, "
    echo "Press y to visit the GDAL download Page, or any other key to cancel"
    read -n 1 yn
    if [ yn == 'y' ]; then
        open 'http://www.kyngchaos.com/software/frameworks#gdal_complete'
        exit 0
    else
        exit 1
    fi
fi

echo "--- Installing with Homebrew"
rc=0
brew install git
let rc=rc+$?
brew install hdf5
let rc=rc+$?
brew install spatialite-tools
let rc=rc+$?

if [ $rc -ne 0 ]; then
	echo "ERROR: one of the brew packages didn't install correctly"
	exit 1
fi
