#!/usr/bin/env bash
# Initialize the Ambry config with environmental vars.
# AMBRY_DB a DSN for a library database

if [ ! -z $AMBRY_DB ]
then
    ambry config edit library.database=$AMBRY_DB
fi

exec bash