#!/usr/bin/env bash


apt-get update
apt-get install -y language-pack-en build-essential make gcc wget curl git
apt-get install -y python python-dev python-pip libffi-dev sqlite3 libpq-dev
apt-get install -y python libgdal-dev gdal-bin python-gdal python-numpy python-scipy
apt-get install -y libsqlite3-dev libspatialite5 libspatialite-dev spatialite-bin libspatialindex-dev
apt-get install -y libhdf5-7 libhdf5-dev
apt-get install -y runit
apt-get clean && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

# Fixes security warnings in later pip installs. The --ignore-installed bit is requred
# because some of the installed packages already exist, but pip 8 refuses to remove
# them because they were installed with distutils.

pip install --upgrade pip && pip install --ignore-installed requests

export LANGUAGE=en_US.UTF-8
export LANG=en_US.UTF-8
export LC_ALL=en_US.UTF-8
locale-gen en_US.UTF-8
dpkg-reconfigure locales

groupadd ambry
usermod -G ambry vagrant

pip install git+https://github.com/clarinova/pysqlite.git#egg=pysqlite

pip install git+https://github.com/CivicKnowledge/ambry.git@develop
su - vagrant -c 'ambry config install'

pip install git+https://github.com/CivicKnowledge/ambry-admin.git
su - vagrant -c 'ambry config installcli ambry_admin'

pip install git+https://github.com/CivicKnowledge/ambry-ui.git
su - vagrant -c 'ambry config installcli ambry_ui'

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

