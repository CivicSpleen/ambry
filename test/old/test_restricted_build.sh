#!/bin/bash 


function abr 
{
    #python -mambry.cli "$@"
    coverage run -a -m ambry.cli "$@"
}

root_dir=$(ambry info | perl -n -e ' print $1 if /Root dir\s*:\s*(.*)/ ')
source_dir=$(ambry info | perl -n -e ' print $1 if /Source\s*:\s*(.*)/ ')
library_file=$(ambry info | perl -n -e ' print $1 if /Library\s*:\s*(.*)/ ')
warehouse_dir=$(ambry info | perl -n -e ' print $1 if /Whs Cache\s*:\s*(.*)/ ')


function init
{
    rm -f .coverage
    
    if [  ! -z "$source_dir" -a -d $source_dir ]
    then
        rm -rf $source_dir
        mkdir -p  $source_dir
    fi
  
    #ambry library drop 
    abr sync
    abr search -R
}


function init_source {
    
    cd $source_dir
    
    # Remove some OSHPD bundles so we have something to build. 
    ambry list -Fvid oshpd   | xargs ambry library remove -a 
    
    git clone https://github.com/CivicKnowledge/ca-health-bundles.git
    git clone https://github.com/CivicKnowledge/ambry-bundles-private.git
    abr library sync -s
    
    # A lot of these fail to build
    abr library remove -a d031004
    abr library remove -a d032005
    abr library remove -a d03D003
    abr library remove -a d03m002
    abr library remove -a d03B002
    
    abr library remove -a d03V005
    abr library remove -a d038007
    abr library remove -a d03R003
    abr library remove -a d039005
    abr library remove -a d037002
    
    abr library remove -a d03a003 
    abr library remove -a d03s003 
    abr library remove -a d03Q006 
             
    
}

function build_all {
    
    while true; do
        abr source buildable && ambry source buildable -Fvid | xargs -n1 -I {} python -m ambry.cli bundle -d {} build --clean --install

        if [ $? != 0 ]; then
            exit 0
        fi

    done
    
}

init
init_source
build_all

#ambry config install -e "environment.category = $source_dir"