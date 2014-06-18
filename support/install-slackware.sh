#!/bin/bash


if [ ! $UID = 0 ]; then
cat << EOF

This script must be run as root.

EOF
  exit 1
fi


if [ -z "$( ls /var/log/packages/ | grep sbopkg )" ]; then
cat << EOF

sbopkg must be installed.
http://www.sbopkg.org/

EOF
  exit 1
fi

DATA_DIR=/data # Directory to store downloads and library.
while getopts "d:" OPTION
do
case $OPTION in
         h)
             usage
             exit 1
             ;;
         d)
             DATA_DIR="-i $OPTARG"
             ;;
         ?)
             usage
             exit
             ;;
     esac
done
shift $((OPTIND-1))


sbopkg -B -r

groupadd -g 209 postgres
useradd -u 209 -g 209 -d /var/lib/pgsql postgres

#if [ "$( uname -m )" = "x86_64" ]; then
#  wget -N http://slackware.org.uk/slacky/slackware64-14.0/database/postgresql/9.2.3/postgresql-9.2.3-x86_64-3sl.txz -P /tmp/
#  installpkg /tmp/postgresql-9.2.3-x86_64-3sl.txz
#else
#  wget -N http://slackware.org.uk/slacky/slackware-14.0/database/postgresql/9.2.1/postgresql-9.2.1-i486-1sl.txz -P /tmp/
#  installpkg /tmp/postgresql-9.2.1-i486-1sl.txz
#fi

## if postgresql fails to build, you may uncomment
## the above code block to install a binary package
## (make sure to comment out the below postgresql line)
sbopkg -B -i postgresql
sbopkg -B -i pysetuptools -i pip -i proj -i geos -i gdal


echo "--- Install the databundles package from github"
# Download the data bundles with pip so the code gets installed.

pip install -r https://raw.github.com/clarinova/databundles/master/requirements.txt

pip install ambry

ambry config install # Installs a development config

cd $(ambry config value filesystem.source)

[ ! -e clarinova-public ] && git clone https://github.com/clarinova/ambry-bundles-public.git clarinova-public

