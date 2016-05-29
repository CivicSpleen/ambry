#!/bin/bash 

which clang > /dev/null

if [ $? -ne 0 ]; then
    echo
    echo "ERROR: First, install XCode and the command line tools to get the C compiler. "
    exit 1
fi

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


##
## Install packages with brew that are required to build python packages.
##

# To deal with recent changes in clang.
#export ARCHFLAGS="-Wno-error=unused-command-line-argument-hard-error-in-future"

echo "--- Installing packages with Homebrew"

brew_packages="git postgresql"

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

# Upgrade setuptools
curl  https://bitbucket.org/pypa/setuptools/raw/bootstrap/ez_setup.py | sudo python


sudo easy_install pip

# The ARCHFLAGS bit handles an change to Apple's clang compiler, affecting many packages in fall 2013 to Spring 2014
sudo ARCHFLAGS="-Wno-error=unused-command-line-argument-hard-error-in-future" \
pip install -r https://raw.githubusercontent.com/clarinova/ambry/master/requirements.txt



