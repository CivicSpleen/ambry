#!/usr/bin/env bash
#
# Build run an Ubuntu docker container and load Ambry. The preferred way to run ambry in Docker is to use
# the `ambry docker build` service from the admbry_admin package, so this one is more intended for
# testing, but it does build a functional Ambry instance.
#
# Note, this installs Ambry in a running docker container; it does not create a docker image.

image=ubuntu:16.04

this_dir="$(cd "$(dirname "$0")"; pwd)"
root_dir="$( dirname $(dirname $this_dir))"

docker run -ti -v $root_dir:/tmp/ambry $image /bin/bash -c "/bin/bash /tmp/ambry/support/install/install-ubuntu.sh; /bin/bash"
