#!/bin/sh

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd -P )"
DIR=$(dirname $(dirname $DIR))
COUNT=$(git rev-list HEAD --count)

cat $DIR/ambry/__init__.py | \
perl -pe "s/^__version__\s*=\s*'(\d+)\.(\d+).*/__version__ = '\1.\2.$COUNT'/" > $DIR/ambry/__init__.py.new

mv $DIR/ambry/__init__.py.new $DIR/ambry/__init__.py
