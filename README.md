# mds-provider-store

> Docker container running an [MDS Provider][provider] data store on PostgreSQL 10.5

## Setup

First clone the repo:

```bash
$ git clone https://github.com/CityofSantaMonica/mds-provider mds-provider
Cloning into 'mds-provider'
remote: Counting objects: xxxx, done.
...
$ cd mds-provider/store
```

Next, create a Docker Data Volume and build the `mds-provider-store` Docker image:

```bash
$ ./docker-setup.sh
```

Finally, run the `mds-provider-store` container:

```
$ ./docker-run.sh
```

**NOTE:** the above assumes a `pg.env` file is present in the directory, with your configuration. See our `pg.env.sample` for an example.

[provider]: https://github.com/CityOfLosAngeles/mobility-data-specification/tree/master/provider