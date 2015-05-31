#!/bin/bash 


function abr 
{
    #python -mambry.cli "$@"
    coverage run -a -m ambry.cli "$@"
}

root_dir=$(ambry info | perl -n -e ' print $1 if /Root dir\s*:\s*(.*)/ ')
source_dir=$(ambry info | perl -n -e ' print $1 if /Source\s*:\s*(.*)/ ')
library_file=$(ambry info | perl -n -e ' print $1 if /Library\s*:\s*(.*)/ ')

function init
{
    rm -f .coverage
    
    if [  ! -z "$root_dir" -a -d $root_dir ]
    then
        rm -rf $root_dir
    fi
    
    abr config install -f 
    abr sync
    abr search -R
}

function init_source {
    
    pushd $source_dir
    git clone https://github.com/CivicKnowledge/example-bundles.git
    abr library sync -s
    
    # These Get installed from remote
    abr library remove -a dfY0NJscai003
    abr library remove -a dHSyDm4MNR002

    # These fail to build or arent necessary
    abr library remove -a ds76uxGOHk001
    abr library remove -a d461o4Mgqr002
    abr library remove -a d00G001
    
}

function build_all {
    
    while true; do
        abr source buildable && ambry source buildable -Fvid | xambry build --clean --install

        if [ $? != 0 ]; then
            exit 0
        fi

    done
    
}

init
init_source
build_all