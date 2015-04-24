#!/bin/sh
# Counts the number of commits and adds that number to the last number of the version
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd -P )"
DIR=$(dirname $(dirname $DIR))
COUNT=$(git rev-list HEAD --count)

cat $DIR/ambry/_meta.py | \
perl -pe "s/^__version__\s*=\s*'(\d+)\.(\d+).*/__version__ = '\1.\2.$COUNT'/" > $DIR/ambry/_meta.py.new

mv $DIR/ambry/_meta.py.new $DIR/ambry/_meta.py
git add $DIR/ambry/_meta.py

