# PostgtreSQL 9.3
#
# VERSION    1.2.2

FROM civicknowledge/ambry-base

MAINTAINER Eric Busboom "eric@civicknowledge.org"

LABEL description="PostgreSQL Server 9.3"

RUN apt-get update \
  && apt-get install -y \
  unzip \
  inotify-tools \
  postgresql-9.3 \
  postgresql-server-dev-9.3 \
  postgresql-contrib-9.3 \
  postgresql-9.3-postgis-2.1 \
  postgresql-client-9.3 \
  pgxnclient \
  supervisor \
  && rm -rf /var/lib/apt/lists/*

RUN pip install requests[security]

RUN mkdir -p /var/run/supervisor \
  && chown -R postgres:postgres /var/run/supervisor

ADD docker-assets/ /

RUN chown postgres:postgres /usr/local/bin/postgres.sh /usr/local/etc/pg_backup.config \
  && chmod +x /usr/local/bin/postgres.sh \
  && chmod +x /usr/local/bin/pg_backup.sh \
  && chmod +x /usr/local/bin/log_watch.sh \
  && chown -R postgres:postgres /var/run/postgresql /var/backups /usr/local/etc

# Locale setting
ENV LOCALE en_US.UTF-8

# Initial default user/pass and schema
ENV USER postgres
ENV PASSWORD postgres
ENV SCHEMA postgres
ENV POSTGIS false
ENV ENCODING UTF8

# Database backup settings FOr backups to run, you also need
# to set the BACKUP_EMAIL var
ENV BACKUP_ENABLED false
ENV BACKUP_FREQUENCY daily

RUN echo "listen_addresses='*'" >> /etc/postgresql/9.3/main/postgresql.conf \
  && echo "host all  all    0.0.0.0/0  md5" >> /etc/postgresql/9.3/main/pg_hba.conf

VOLUME	["/etc/postgresql", "/var/log/postgresql", "/var/lib/postgresql", "/var/backups"]

RUN touch /var/lib/postgresql/firstrun && chmod 666 /var/lib/postgresql/firstrun

EXPOSE 5432

##
## Install Ambry FDW


WORKDIR /tmp

# Shouldn't really need Sqlite, but must have v 3.8.8 to install a dependency
# in ambry_sources
RUN wget http://www.sqlite.org/2015/sqlite-autoconf-3090200.tar.gz && \
    tar -xzvf sqlite-autoconf-3090200.tar.gz && \
    cd sqlite-autoconf-3090200 && \
    ./configure && make && make install

RUN pgxn install multicorn

RUN pip install ambry_sources[geo,fdw]

RUN  usermod -G ambry postgres

CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/supervisord.conf"]
