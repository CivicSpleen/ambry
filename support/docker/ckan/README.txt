
To Build:

    docker build -t civicknowledge/ckan_solo . 

docker run \
-e 'SITE_URL=ckan.sandiegodata.org' -e "ADMIN_USER_PASS=password"  \
-e "ADMIN_USER_EMAIL=eric@busboom.org" -e "ADMIN_USER_KEY=apikey"  \
-P --name ckan_solo civicknowledge/ckan_solo

For development run with:

docker run \
-e 'SITE_URL=ckan.sandiegodata.org' -e "ADMIN_USER_PASS=password"  \
-e "ADMIN_USER_EMAIL=eric@busboom.org" -e "ADMIN_USER_KEY=apikey"  \
-e "DEBUG=1" \
-P --rm -t -i --name ckan_solo civicknowledge/ckan_solo

Run a server that accesses the volumes: 

    docker run --rm -t -i --volumes-from ckan ubuntu /bin/bash

