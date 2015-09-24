#!/usr/bin/env bash
# Change directories to a bundle. If the bundle ref is not given, use the
# currently active bundle, as reported by `bambry info -w`

bambrycd() {

    if [ -z "$1" ]; then
        dir=$(bambry info  -w -q )
    else
        dir=$1
    fi

    cd `bambry -i $dir info -s`
}
