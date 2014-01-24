#/bin/sh

ver=0.0.2

rm -rf build/ Databundles.egg-info/;
python setup.py clean build sdist; 
cp dist/databundles-$ver.tar.gz  /net/nas2/c/proj/python


ssh root@lorne 'source /data/bundles/bin/activate; pip uninstall -y databundles' 
ssh root@lorne "source /data/bundles/bin/activate; pip install --upgrade /net/nas2/c/proj/python/databundles-$ver.tar.gz"


rsync -av --exclude='build*/'  --exclude='.git' /Users/eric/proj/github.com/civicdata/ root@lorne:/build/github.com/civicdata/
