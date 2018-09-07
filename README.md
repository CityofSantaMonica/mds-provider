# mds-provider-store

Docker containers for running an [MDS Provider][provider] data store.

## Getting started

Requires both [Docker][docker] and [Docker Compose][compose] for local development.

### Create an `.env` file

Copy the sample and edit as necessary.

```bash
$ cp .env.sample .env
```

### Start the containers

Start a local PostgreSQL server, as well as a local pgAdmin 4 client.

```bash
$ docker-compose up -d --build --force-recreate
```

Now head to http://localhost:$PGADMIN_HOST_PORT to connect.

### Stop the container

Shutdown and completely erase the containers

```bash
$ docker-compose down
```

[compose]: https://docs.docker.com/compose/overview/
[docker]: https://www.docker.com/
[provider]: https://github.com/CityOfLosAngeles/mobility-data-specification/tree/master/provider
