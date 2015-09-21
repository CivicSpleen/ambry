Create a New Bundle
===================

bambry new -s nbcuni.com -d streetsweep 

If you sourced the ambry-aliases.sh, you can cd to the bundle directory with:

bambrycd

The new bundle directory will be empty; Ambry works primarily off of its database, so you occasionally
need to be explicit about synchronizing. 

bambry sync 

You should have some files in the directory now:

    $ ls -1
    bundle.yaml
    schema.csv
    source_schema.csv
    sources.csv
    
    