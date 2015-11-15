#!/usr/bin/env bash
# Initialize the Ambry config with environmental vars.
# AMBRY_DB: a DSN for a library database

# Edit just the database
if [ ! -z $AMBRY_DB ]
then
    ambry config edit library.database=$AMBRY_DB
fi

# Edit the entire file, using a JSON input

if [ ! -z $AMBRY_CONFIG_EDIT ]
then
    ambry config edit -j $AMBRY_CONFIG_EDIT
fi


exec bash