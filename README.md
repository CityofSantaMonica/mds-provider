# mds-provider

Docker containers for running an [MDS `provider`][provider] data store.

## Local development

Requires both [Docker][docker] and [Docker Compose][compose].

### Create an `.env` file

Copy the sample and edit as necessary.

```bash
$ cp .env.sample .env
```

### Start the containers

Start a local PostgreSQL server, as well as a local pgAdmin4 client. Runs initialization
scripts to create the database `$MDS_DB` with user account info `$MDS_USER` and `$MDS_PASSWORD`.

```bash
$ docker-compose up -d --build --force-recreate
```

Now browse `http://localhost:$PGADMIN_HOST_PORT` to connect.

### Stop the containers

Shutdown and completely erase the containers.

```bash
$ docker-compose down
```

[compose]: https://docs.docker.com/compose/overview/
[docker]: https://www.docker.com/
[provider]: https://github.com/CityOfLosAngeles/mobility-data-specification/tree/master/provider
