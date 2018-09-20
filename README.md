# mds-provider

Services for working with [MDS `provider`][provider] data.

## Local development

Requires both [Docker][docker] and [Docker Compose][compose].

`docker-compose` commands below should be run from the root of this repository,
where the `docker-compose.yml` file lives.

### Container organization

The containers are organized around specific functions. More detailed explanation
can be found in a container's `README.md`.

| container | description |
| --------- | ----------- |
| [`client`](client/)  | [pgAdmin4][pgadmin] web client |
| [`fake`](fake/)    | Generate fake MDS `provider` data for testing and development |
| [`initdb`](initdb/)  | Initialize an MDS `provider` database |
| [`server`](server/)  | Local [postgres][postgres] database server |

### 1. Create an `.env` file

Copy the sample and edit as necessary. Compose automatically sources this
environment file for `docker-compose` commands.

```bash
$ cp .env.sample .env
```

### 2(a). Start (all of the) containers

Build and start all of the containers according to the dependencies outlined in
[`docker-compose.yml`](docker-compose.yml).

```bash
$ docker-compose up -d --build --force-recreate
```

### 2(b). Start (individual) containers

See the `README.md` file in each container folder for more details.

### 3. Stop the containers

Shutdown and completely erase the containers and their resources.

```bash
$ docker-compose down
```

[compose]: https://docs.docker.com/compose/overview/
[docker]: https://www.docker.com/
[pgadmin]: https://www.pgadmin.org/
[postgres]: https://www.postgresql.org/
[provider]: https://github.com/CityOfLosAngeles/mobility-data-specification/tree/master/provider
