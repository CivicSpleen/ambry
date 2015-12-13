
FROM civicknowledge/ambry-base

MAINTAINER Eric Busboom "eric@civicknowledge.org"

COPY . /opt/ambry/
WORKDIR /opt/ambry/

RUN python setup.py install

WORKDIR /var/ambry
VOLUME /data

# These really ought to be in the requirements file
RUN pip install -r /opt/ambry/requirements/numbers.txt

EXPOSE 80

CMD python -m ambry.server.numbers