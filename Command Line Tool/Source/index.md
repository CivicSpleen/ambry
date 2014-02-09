
# Ambry Source

## Using Find

ambry source find -P identity.source like sangis | ambry bundle -d - build 

ambry source find -P identity.source like sangis | xargs -n1 ambry source get