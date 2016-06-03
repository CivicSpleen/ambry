#!/usr/bin/env bash
#
# Build run an Ubuntu docker container and load Ambry. The preferred way to run ambry in Docker is to use
# the `ambry docker build` service from the admbry_admin package, so this one is more intended for
# testing, but it does build a functional Ambry instance.
#
# Note, this installs Ambry in a running docker container; it does not create a docker image.
#
# WARNING: This only works when docker is running locally, because of the volumn mount. If docker is not
# running locally, you can use docker to create a new ubuntu container, and install in it manually.

container_name=${1:-'ambry'}

image=ubuntu:16.04

this_dir="$(cd "$(dirname "$0")"; pwd)"
root_dir="$( dirname $(dirname $this_dir))"


docker run -ti --name $container_name -v $root_dir:/tmp/ambry $image /bin/bash # /tmp/ambry/support/install/install-ubuntu.sh
