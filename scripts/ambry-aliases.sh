#!/usr/bin/env bash
# Change directories to a bundle. If the bundle ref is not given, use the
# currently active bundle, as reported by `bambry info -w`
#
# Options
#    -s cd to the Source directory (default)
#    -b cd to the build directory
bambrycd() {

    OPTIND=1         # Reset in case getopts has been used previously in the shell.

    cd_opt='-s'
    while getopts "sb" opt; do
        cd_opt="-$opt"
    done

    shift $((OPTIND-1))

    if [ -z "$1" ]; then
        dir=$(bambry info  -w -q -H ) # Get the current bundle
    else
        dir=$1
    fi

    cd $(bambry -i $dir info -T $cd_opt)
}

#
# Set the AMBRY_DB environmental variable based on a docker group name
#
ambry_switch() {

    if [ -n "$1" ]; then
        docker_db=$(ambry docker info -d $1)
    fi


    if [ -z "$1" ]; then
        unset AMBRY_DB
    elif [ -n "$docker_db" ]; then
        export AMBRY_DB=$docker_db
    else
        echo Error: No database for reference \'$1\'
    fi
}