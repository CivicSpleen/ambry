
FROM civicknowledge/ambry-base

MAINTAINER Eric Busboom "eric@civicknowledge.org"

ENV AMBRY_IS_REMOTE 1

COPY . /opt/ambry/
WORKDIR /opt/ambry/

RUN python setup.py install

WORKDIR /var/ambry

VOLUME /var/ambry

RUN mkdir -p /var/ambry/source

ADD ${AMBRY_CONFIG:-ambry/support/ambry-docker.yaml} /etc/ambry.yaml
RUN echo 'source /usr/local/bin/ambry-aliases.sh' >> /root/.bashrc

# Could probabaly run this out of the support directory too.
ADD support/docker/ambry/ambry-init.sh /opt/ambry/ambry-init.sh


CMD bash /opt/ambry/ambry-init.sh