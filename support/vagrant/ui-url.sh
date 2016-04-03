#!/usr/bin/env bash

IP=$(vagrant ssh -c "ip address show | grep 'inet ' | sed -e 's/^.*inet //' -e 's/\/.*$//' | grep 192")
echo "IP Address $IP"
URL="http://$IP"

echo open $URL

