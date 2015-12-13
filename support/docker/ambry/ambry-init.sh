#!/usr/bin/env bash
# Initialize the Ambry config with environmental vars.

# Edit the entire file, using a JSON input

if [ ! -z $AMBRY_CONFIG_EDIT ]
then
    ambry config edit -j $AMBRY_CONFIG_EDIT
fi



# Run an ambry command in the container
if [ ! -z "$AMBRY_COMMAND" ]
then
    exec $AMBRY_COMMAND
else
    exec bash
fi

