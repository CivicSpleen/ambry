# A container for storing ambry build files and database backups.
# cogniteev/echo and tianon/true are much smaller
FROM  busybox


RUN mkdir /var/ambry && mkdir /var/backups

VOLUME ['/var/ambry', '/var/backups']

RUN addgroup -g 1000 ambry && chgrp ambry /var/ambry && chmod g+rwxs /var/ambry


MAINTAINER Eric Busboom "eric@civicknowledge.org"
