#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd -P )"

cd $DIR

source_dir=$1
strip=$2

if [ -z "$strip" ]
then
	strip='string-that-will-never-appear-in-a-path'
fi	
if [ ! -d "$source_dir" ]
then
    echo "ERROR parameter is not a directory: $source_dir"
	exit 1 
fi	

files=$(find $source_dir -name '.ipynb_checkpoints' -prune -o -name '*.ipynb' -print)

IFS=$'\n'
for f in $files
do
	dest_final=$(echo $f | perl -p -e "s#\.\.?./##; s# #_#g; s#^$strip##"  )
	dest=$DIR/notebooks/$dest_final
	dest_dir=$(dirname $dest)
	mkdir $dest_dir
	ipython nbconvert --to html --output $dest $f
	
done
