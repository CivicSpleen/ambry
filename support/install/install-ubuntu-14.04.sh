#!/usr/bin/env bash

set -e

SUDO=

$SUDO apt-get update
$SUDO apt-get install -y language-pack-en build-essential make gcc wget curl git
$SUDO apt-get install -y python python-dev python-pip libffi-dev sqlite3 libpq-dev
$SUDO apt-get install -y python libgdal-dev gdal-bin python-gdal python-numpy python-scipy
$SUDO apt-get install -y libsqlite3-dev libspatialite5 libspatialite-dev spatialite-bin libspatialindex-dev
$SUDO apt-get install -y libhdf5-7 libhdf5-dev
$SUDO apt-get install -y runit
$SUDO apt-get clean && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

# Fixes security warnings in later pip installs. The --ignore-installed bit is requred
# because some of the installed packages already exist, but pip 8 refuses to remove
# them because they were installed with distutils.

$SUDO pip install --upgrade pip && pip install --ignore-installed requests

export LANGUAGE=en_US.UTF-8
export LANG=en_US.UTF-8
export LC_ALL=en_US.UTF-8
$SUDO locale-gen en_US.UTF-8
$SUDO dpkg-reconfigure locales

$SUDO groupadd ambry
$SUDO usermod -G ambry ubuntu # ubuntu user is particular to AWS

$SUDO mkdir -p /opt/ambry

$SUDO pip install git+https://github.com/clarinova/pysqlite.git#egg=pysqlite

#pip install git+https://github.com/CivicKnowledge/ambry.git@develop
mkdir -p /opt/ambry
cd /opt/ambry
git clone https://github.com/CivicKnowledge/ambry.git
cd ambry
git checkout develop
# On AWS, gets compile errors in numpy if we don't do this first
pip install -r requirements.txt
python setup.py install

ambry config install

pip install git+https://github.com/CivicKnowledge/ambry-admin.git
ambry config installcli ambry_admin

pip install git+https://github.com/CivicKnowledge/ambry-ui.git
ambry config installcli ambry_ui

echo 'source /usr/local/bin/ambry-aliases.sh' >> /root/.bashrc


mkdir -p /etc/sv
cp -r ../vagrant/service/* /etc/sv

mkdir -p /var/log/ambry/notebook
mkdir -p /var/log/ambry/ambryui

adduser log --system --disabled-password

chown log /var/log/ambry/ambryui
chown log /var/log/ambry/notebook

ln -s /etc/sv/ambryui /etc/service
ln -s /etc/sv/notebook /etc/service

chown -R root.ambry  /var/ambry
chmod g+rw /var/ambry

