
Running With Docker
===================

Using docker with Amby makes it much easier to build bundles with multiple processes on multiple machines.

Ambry runs in docker with these containers:

- A volume container, for holding downloaded files and built bundles
- A postgres database container, which holds the library database and warehouses
- One or more transient builders containers, to build bundles and run other Ambry commands.
- A UI container, to run the web interface to the library.

These containers can be build and run using either the :command:`python setup.py docker` command, or with :command:`docker compose`. Additionally, the :command:`bambry docker` command allows running Ambry commands in a transient container, automatically connected to the database and volume container.


Using Docker Compose
********************

The source distribution includes a configuration file for :command:`docker compose` in :file:`support/docker`. This compose script is particularly useful for setting up the data container and postgres database, and building the ambry builder.

.. code-block:: bash

    $ cd support/docker
    $ docker-compose up


Using Setup.py
**************

The :command:`python setup.py docker` command is used to build and tag images for the various container types. The command has these options, each to build a different container type:

- :option:`--base | -B` Builds the base container, `civicknowledge/base`, used by other containers.
- :option:`--build | -b` Builds the builder container, which is launched from :command:`bambry docker`
- :option:`--dev | -d` BUilds the same container as - :option:`--build,` but installs some packages from git rather than pip, so it is easier to use in development.



Create a Data Volume
********************

The database and builder containers should have access to the same data volume, to ensure that the database can read from data files. To create a data volume, using the host directory '/data'

.. code-block:: bash

    $ docker create -v /data:/var/ambry --name ambrydocker_volumes ubuntu /bin/true

You can also use the docker compose script to create a docker container.

    $ cd support/docker
    $ docker-compose up volumes

Note that in the first example, the data will be acessible on the host at :file:`/data', while in the second, it is contained in a docker volume and does not have a host directory. If you delete the container created with :command:`docker-compose`, be sure to use the :option:`-v` option to :command:`docker rm` to also remove the volume. Otherwise, you'll have an orphaned volume that is hard to remove.


Either way, the volume container is named `ambrydocker_volumes`, which you can set in the :option:`docker.volumes_from`. Then you can use the :option:`--volumes-from` option to :command:`docker run` to use the volume in other containers.



Configuration
*************

Environmental Variables
-----------------------

The docker container for building bundles uses several environmental variables to configure it's operation.

- :envvar:`AMBRY_DB` Database DSN for the AMbry library
- :envvar:`AMBRY_ACCOUNT_PASSWORD` The password for decrypting account secrets
- :envvar:`AMBRY_LIMITED_RUN` When building in docker, use `-L`
- :envvar:`AMBRY_COMMAND`. Start the container with this ambry command, then exit.



Ambry.yaml Configuration
------------------------

The :file:`ambry.yaml` file can have a few configuration items that effect
operation of docker containers. 

The :option:`docker.volumes_from` config specifies a single argument for the :option:`--volumes-from` argument when running :command:`bambry docker`. The option allows for creating a volume container to hold build files. You'll nearly always want to set this value; if it isn't set, the files created during a build will be lost when the container exits.

The :option:`docker.ambry_image` config specified the image that is used when running :command:`bambry docker`. This config is useful to set :command:`bambry docker` to use the image created with  :command:`docker compose`

If you use :command:`docker compose` to create the docker images instead of :command:`python setup.py docker`, these configuration values will be useful to ensure :command:`bambry docker` uses the images created by  :command:`docker compose`.

.. code-block: yaml

    docker:
        volumes_from: ambrydocker_volumes
        ambry_image: ambrydocker_ambry
        ui_domain: barker.local
        

Other Issues
************

UI Proxies
----------

The UI containers are hard to use if you have to run :command:`docker ps` to find the host port, so it is more useful to setup a web proxy to rount we requests to the host to the UI containers. The ``jwilder/nginx-proxy`` is an easy way set up these proxies automatically. When the ui containers are created, a :envvar:`VIRTUAL_HOST` environmental value is automatically set, so the ``jwilder/nginx-proxy`` can automatically configure a proxy entry.

