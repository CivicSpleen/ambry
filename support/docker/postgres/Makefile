#include Makefile.local
NS = civicknowledge
VERSION = latest

REPO = postgres
NAME = postgres
INSTANCE = default
DOCKER ?= docker

.PHONY: build push shell run start stop restart reload rm rmf release

build:
	$(DOCKER) build -t $(NS)/$(REPO):$(VERSION) .

push:
	$(DOCKER) push $(NS)/$(REPO):$(VERSION)

shell:
	$(DOCKER) run --rm --name $(NAME) -i -t $(PORTS) $(VOLUMES) $(ENV) $(NS)/$(REPO):$(VERSION) /bin/bash

run:
	$(DOCKER) run --rm --name $(NAME) $(PORTS) $(VOLUMES) $(ENV) $(NS)/$(REPO):$(VERSION)

start:
	$(DOCKER) run -d --name $(NAME) $(PORTS) $(VOLUMES) $(ENV) $(NS)/$(REPO):$(VERSION)

stop:
	$(DOCKER) stop $(NAME)
	
restart: stop start

reload: build rmf start

rmf:
	$(DOCKER) rm -f $(NAME)

rm:
	$(DOCKER) rm $(NAME)

release: build
	make push -e VERSION=$(VERSION)

default: build