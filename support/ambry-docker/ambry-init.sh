#!/usr/bin/env bash
# Initialize the Ambry config with environmental vars.

# Edit the entire file, using a JSON input

if [ ! -z $AMBRY_CONFIG_EDIT ]
then
    ambry config edit -j $AMBRY_CONFIG_EDIT
fi


exec bash