# initdb

Initialize an MDS `provider` database.

## Configuration

This container uses the following environment variables to initialize the MDS database:

```bash
MDS_DB=mds_provider
MDS_USER=mds_provider
MDS_PASSWORD=mds_provider_password
```

## Setup scripts

Run the [setup scripts](bin/) from within the running container directly, or by
using the container in executable form with Compose.

### Initialize the database

Run by default when the container starts up.

```
$ docker-compose run initdb bin/initdb.sh
```

### Reset the database

Tears down the MDS database and then re-initializes.

```
$ docker-compose run initdb bin/reset.sh
```
