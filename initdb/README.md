# initdb

## Setup scripts

Setup scripts can be found in the [`bin/`](bin/) directory. These can be run from
within the running container directly, or by using the container in *executable*
form.

### Initialize the database

Run by default when the container starts up.

```
$ docker-compose run initdb bin/initdb.sh
```

### Reset the database

Tears down the MDS database and then runs `initdb.sh`. Ensure there are no
active connections before running.

```
$ docker-compose run initdb bin/reset.sh
```
