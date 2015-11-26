
To build the docker container, run, from the root of the distribution: 

    $ python setup.py docker-build
    
This will create a docker container tagged 'civicknowledge/ambry' using the version of the code in the distribution.

To run the container:

    $ ambry docker -r
    
The `ambry docker` command will set `AMBRY_DB` to the current database, so it only make sense if the library
database is postgres. 
    
The result will be a shell prompt, with ambry installed, using a local Sqlite database. 

If you want to run another database, use `-e AMBRY_DB`

    $ docker run --rm -t -i -e AMBRY_DB=postgres://<user>:<pass>>@<host>:<port>/ambry civicknowledge/ambry

The AMBRY_DB value can be any valid Ambry database DSN. 

## Database

We usually use the [official postgres release.](https://hub.docker.com/_/postgres/), which can be run with:

    docker run --name ambry_db -p 5432:5432 -e POSTGRES_PASSWORD=ambry -e POSTGRES_USER=ambry -d postgres
    
## Caches

It is valuable to cache downloads, and in a docker environment, doing so requires a persistent storage for the download
cache. 

Create a docker volume container:

    $ docker create -v /data/ambry:/var/ambry --name ambry_fs ubuntu /bin/true
  

  
Where "/data/downloads" is a directory on the docker host. This command will create a new container names 
'downloads' to hold the data. 

Then, when running a container, include `--volumes-from ambry_fs` to attach the volume into the new container. 