#!/usr/bin/env bash

error() {
  local parent_lineno="$1"
  local message="$2"
  local code="${3:-1}"
  if [[ -n "$message" ]] ; then
    echo "Error on or near line ${parent_lineno}: ${message}; exiting with status ${code}"
  else
    echo "Error on or near line ${parent_lineno}; exiting with status ${code}"
  fi
  exit "${code}"
}
trap 'error ${LINENO}' ERR

SUDO=

$SUDO apt-get update
$SUDO apt-get install -y language-pack-en build-essential make gcc wget curl git
$SUDO apt-get install -y python python-dev python-pip libffi-dev sqlite3 libpq-dev
$SUDO apt-get install -y python libgdal-dev gdal-bin python-gdal python-numpy python-scipy
$SUDO apt-get install -y libsqlite3-dev libspatialite5 libspatialite-dev spatialite-bin libspatialindex-dev
$SUDO apt-get install -y libhdf5-7 libhdf5-dev
$SUDO apt-get clean && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

# Fixes security warnings in later pip installs. The --ignore-installed bit is requred
# because some of the installed packages already exist, but pip 8 refuses to remove
# them because they were installed with distutils.

$SUDO pip install --upgrade pip && pip install --ignore-installed requests

# Ambry needs a later version, but it gets installed with python 
$SUDO pip install --upgrade setuptools

export LANGUAGE=en_US.UTF-8
export LANG=en_US.UTF-8
export LC_ALL=en_US.UTF-8
$SUDO locale-gen en_US.UTF-8
$SUDO dpkg-reconfigure locales

if [ $(getent group ambry) ]; then
  echo "group ambry exists."
else
  $SUDO groupadd ambry
fi

if getent passwd ubuntu > /dev/null 2>&1; then
    $SUDO usermod -G ambry ubuntu # ubuntu user is particular to AWS
fi

# Make a fresh install of Ambry
[ -d /opt/ambry ] && $SUDO rm -rf /opt/ambry

$SUDO mkdir -p /opt/ambry

$SUDO pip install git+https://github.com/clarinova/pysqlite.git#egg=pysqlite

#pip install git+https://github.com/CivicKnowledge/ambry.git@develop
[ -d /opt/ambry ] || mkdir -p /opt/ambry
cd /opt/ambry

git clone https://github.com/CivicKnowledge/ambry.git
cd ambry
git checkout develop

# On AWS, gets compile errors in numpy if we don't do this first
pip install -r requirements.txt
python setup.py install

ambry config install -f

pip install git+https://github.com/CivicKnowledge/ambry-admin.git
ambry config installcli ambry_admin

pip install git+https://github.com/CivicKnowledge/ambry-ui.git
ambry config installcli ambry_ui

echo 'source /usr/local/bin/ambry-aliases.sh' >> /root/.bashrc

chown -R root.ambry  /var/ambry
chmod g+rw /var/ambry

# When this script is run for installing vagrant, also install 
# runit and the runit services
if [ -d ./vagrant/service ]; then
  $SUDO apt-get install -y runit
  $SUDO mkdir -p /etc/sv
  $SUDO cp -r ../vagrant/service/* /etc/sv
  
  mkdir -p /var/log/ambry/notebook
  mkdir -p /var/log/ambry/ambryui

  adduser log --system --disabled-password

  chown log /var/log/ambry/ambryui
  chown log /var/log/ambry/notebook

  ln -s /etc/sv/ambryui /etc/service
  ln -s /etc/sv/notebook /etc/service
  
fi


