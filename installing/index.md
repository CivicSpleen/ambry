---
layout: default
title: 'Building Ambry Bundles'
---

# Building 


## Linux


## Development Install

### Create the Virtual Environment

virtualenv ambry
cd ambry/
source bin/activate

pip install -e 'git+https://github.com/clarinova/ambry.git#egg=ambry'


## Configuration

Install the configuration

ambry config install -p -t library \
    library.default.upstream.bucket=foobar \
    library.default.upstream.account=baz