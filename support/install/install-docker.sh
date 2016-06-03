#!/usr/bin/env bash
#
# Build run an Ubuntu docker container and load Ambry. The preferred way to run ambry in Docker is to use
# the `ambry docker build` service from the admbry_admin package, so this one is more intended for
# testing, but it does build a functional Ambry instance.
#
# Note, this installs Ambry in a running docker container; it does not create a docker image.

container_name=${1:-'ambry'}

image=ubuntu:16.04

this_dir="$(cd "$(dirname "$0")"; pwd)"
root_dir="$( dirname $(dirname $this_dir))"

echo $this_dir
echo $root_dir

echo docker run -ti --name $container_name -v $root_dir:/tmp/ambry $image \
    -c "/bin/bash /tmp/ambry/support/install/install-ubuntu.sh; /bin/bash"
