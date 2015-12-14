#
# Base Ambry image to install the most expensive packages, allowing for faster builds and uploads
# images built atop this one.
FROM phusion/baseimage:0.9.17

MAINTAINER Eric Busboom "eric@civicknowledge.org"

RUN apt-get update && apt-get install -y \
    language-pack-en \
    build-essential make gcc \
    wget curl  git \
    python python-pip python-dev libffi-dev \
    sqlite3  libpq-dev \
    libgdal-dev gdal-bin python-gdal python-numpy python-scipy  \
    libsqlite3-dev libspatialite5 libspatialite-dev spatialite-bin libspatialindex-dev && \
    apt-get clean && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip

ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
RUN locale-gen en_US.UTF-8
RUN dpkg-reconfigure locales

# This package allows Sqlalchemy to load the spatialite shared object to provide
# Spatialite services.
RUN pip install git+https://github.com/clarinova/pysqlite.git#egg=pysqlite

COPY requirements /tmp/requirements

RUN groupadd -g 1000  ambry

RUN pip install -r /tmp/requirements/base_py2.txt  && rm -rf ~/.cache/pip/

