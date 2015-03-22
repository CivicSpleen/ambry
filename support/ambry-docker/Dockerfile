#
# docker build -t civick/ambry .

FROM ubuntu:14.04

MAINTAINER Eric Busboom "eric@civicknowledge.org"

RUN apt-get update && apt-get install -y \
    language-pack-en \
    build-essential make gcc \
    wget curl  git \
    python python-pip python-dev \
    sqlite3  libpq-dev \
    libgdal-dev gdal-bin python-gdal python-numpy python-scipy  \
    libsqlite3-dev libspatialite5 libspatialite-dev spatialite-bin libspatialindex-dev


ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
RUN locale-gen en_US.UTF-8
RUN dpkg-reconfigure locales

# This package allows Sqlalchemy to load the spatialite shared object to provide
# Spatialite services.
RUN pip install git+https://github.com/clarinova/pysqlite.git#egg=pysqlite

RUN pip install git+https://github.com/CivicKnowledge/ckcache.git
RUN pip install git+https://github.com/CivicKnowledge/geoid.git
RUN pip install git+https://github.com/CivicKnowledge/ambry.git
RUN pip install git+https://github.com/CivicKnowledge/ambrydoc.git #refresh3

VOLUME /var/ambry
RUN mkdir -p /var/ambry/source
RUN ambry config install

RUN ambry library sync -r