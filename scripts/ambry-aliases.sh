#!/usr/bin/env bash
# Change directories to a bundle. If the bundle ref is not given, use the
# currently active bundle, as reported by `bambry info -w`

bambrycd() {

    OPTIND=1         # Reset in case getopts has been used previously in the shell.

    cd_opt='-s'
    while getopts "sb" opt; do
        cd_opt="-$opt"
    done

    shift $((OPTIND-1))

    if [ -z "$1" ]; then
        dir=$(bambry info  -w -q )
    else
        dir=$1
    fi

    cd `bambry -i $dir info $cd_opt`
}
