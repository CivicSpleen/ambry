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

set_bambry_prompt() {

    local BLUE="\[\033[0;34m\]"
    local DARK_BLUE="\[\033[1;34m\]"
    local RED="\[\033[0;31m\]"
    local DARK_RED="\[\033[1;31m\]"
    local BLACK="\[\033[0;30m\]"
    local WHITE="\[\033[1;37m\]"
    local YELLOW="\[\033[1;33m\]"
    local NO_COLOR="\[\033[0m\]"

    case "$1" in
        red)
          COLOR=$RED
          ;;
        darkred)
          COLOR=$DARK_RED
          ;;
        blue)
          COLOR=$BLUE
          ;;
        darkblue)
          COLOR=$DARK_BLUE
          ;;
        black)
          COLOR=$BLACK
          ;;
        yellow)
          COLOR=$YELLOW
          ;;
        white)
          COLOR=$WHITE
          ;;
        *)
          COLOR=$DARK_BLUE
          ;;
    esac

    PS1="$COLOR$(bambry info -w -q)$NO_COLOR/$COLOR\W$NO_COLOR $ "

}

