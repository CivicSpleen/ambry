
Running With Docker
===================

Using docker with Amby makes it much easier to build bundles with multiple processes on multiple machines.

Ambry runs in docker with these containers:

- A volume container, for holding downloaded files and built bundles
- A postgres database container, which holds the library database and warehouses
- One or more transient builders containers, to build bundles and run other Ambry commands.
- A UI container, to run the web interface to the library.

These containers can be build and run using either the :command:`python setup.py docker` command, or with :command:`docker compose`. Additionally, the :command:`bambry docker` command allows running Ambry commands in a transient container, automatically connected to the database and volume container.

Initial Setup
*************

Of course, you will need to setup docker first. For development and testing on Max and Windows, it is easiest to use a destktop docker environment like `Kitematic <https://kitematic.com/>`_,  while for Linux you can `install docker directly <https://docs.docker.com/linux/step_one/>`_.

After you have docker working, you will need to:
- Build all of the Ambry docker images on yout host
- Create a container group

Building Images
***************

The :command:`python setup.py docker` command is used to build and tag images for the various container types. The command has these options, each to build a different container type:

- :option:`--base | -B` Builds the base container, `civicknowledge/base`, used by other containers.
- :option:`--build | -b` Builds the builder container, which is launched from :command:`bambry docker`
- :option:`--dev | -d` Builds the same container as - :option:`--build,` but installs some packages from git rather than pip, so it is easier to use in development.
- :option:`--db | -D` Builds the postgres database image
- :option:`--tunnel | -t` Builds the image for an SSH tunnel to access remote databases securely.
- :option:`--ui | -u` Builds the image for the web user interface to a library;
- :option:`--volumes | -v` Builds the image for containers that hold data for a group of related containers.

Or, just use :option:`-a` to build everything.

The - :option:`--build | -b` and - :option:`--dev | -d` options build the same container, with the same tags, so only one is required. The `dev` image gets some very actively developed modules from github, while the `build` image gets them from :command:`pip`, so the `dev` image is preferred for use in development.


Create a Container Group
************************

A container group is a colelction of interacting Ambry container for a single library. You will create a container group for each seperate library you want to work with. The groups have a group name, which you can set with the :option:`-g | --groupname` option, or you can let the system choose a random name.

To create a container group, run :command:`ambry docker init`. If you are running in a non-public environment, use the :option:`-p | --public` option to add a port marring for the database container. In a public environment -- for instance, your docker host is at AWS or Digital Ocean --  omit and  :option:`-p | --public` option. In this case, you will need to use the :command:`ambry docker tunnel` command to create a secure SSH tunnel to remotely access your database, or perform Ambry operations from a container on the same host.

To create a container group for the group name `demo`, with a public database port, run:

.. code-block:: bash

    $ ambry docker init -g demo -p -m'This is a demo library'

After the container group is created, you will need to configure the database DSN for the new database. You can either set the DSN that is print to the screen in the :option:`library.database` config in your :file:`.ambry.yaml` file, or you can set the :envvar:`AMBRY_DB` environmental variable. Set the :option:`library.database` if you only expect to work with one database, and set  :envvar:`AMBRY_DB` environmental variable if you expect to work with many.

If you have setup the :file:`ambry-aliases.sh` file ( find it with :command:`which ambry-aliases.sh`, then source it in your :file:`.bashrc` or :file:`.bash_profile` ) you can run the :command:`ambry_switch` function to set the :envvar:`AMBRY_DB` environmental variable to the DSN associated with a group name. For instance, after creating the `demo` group:

.. code-block:: bash

    $ source `which ambry-aliases.sh`
    $ ambry_switch demo
    $ printenv AMBRY_DB
    postgres://demo:ume9qnwwlgxl@192.168.1.30:32827/26joo6xskj05?docker


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

.. code-block:: yaml

    docker:
        volumes_from: ambrydocker_volumes
        ambry_image: ambrydocker_ambry
        ui_domain: barker.local
        

Other Issues
************

UI Proxies
----------

The UI containers are hard to use if you have to run :command:`docker ps` to find the host port, so it is more useful to setup a web proxy to rount we requests to the host to the UI containers. The ``jwilder/nginx-proxy`` is an easy way set up these proxies automatically. When the ui containers are created, a :envvar:`VIRTUAL_HOST` environmental value is automatically set, so the ``jwilder/nginx-proxy`` can automatically configure a proxy entry.

