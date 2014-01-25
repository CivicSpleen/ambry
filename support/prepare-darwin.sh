#!/bin/bash 

echo "--- Installing base packages. May need to ask for root password"

#
# Install scikit, scipy, numpy and others, on Mac OS X
#  curl -o install_superpack.sh https://raw.github.com/fonnesbeck/ScipySuperpack/master/install_superpack.sh
#  sh install_superpack.sh
#

command -v brew >/dev/null 2>&1

if [ $? -ne 0 ]; then
    echo "ERROR: This script requires the bew package manager "
    echo "Recommended to install Homebrew with: "
    echo '  ruby -e "$(curl -fsSL https://raw.github.com/mxcl/homebrew/go)"'
    echo
    exit 1
fi


which clang > /dev/null

if [ $? -ne 0 ]; then
    echo "ERROR: First, install XCode and the command line tools to get the C compiler. "
    exit 1	
fi	

gdal_version=$(python -c 'import gdal; print gdal.VersionInfo()')

if [ $? -ne 0 ]; then
    echo "ERROR: GDAL not found. Install the KyngChaos GDAL framework, from http://www.kyngchaos.com/files/software/frameworks/GDAL_Complete-1.9.dmg"
    exit 1	
fi	

if [ $gdal_version -lt 1920 ]; then
    echo "ERROR: GDAL Found, but version $gdal_version is too old. Upgrade with KyngChaos frame work, "
    echo " from:http://www.kyngchaos.com/files/software/frameworks/GDAL_Complete-1.9.dmg"
    exit 1			
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
