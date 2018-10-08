# load

Load from `provider` data files into a data store.

## Running

Run the container to load data from one or more sources.

First, ensure the image is up to date locally:

```bash
$ docker-compose build --no-cache load
```

Then run the data loader:

```bash
$ docker-compose run --rm load [--status_changes <path(s)>] [--trips <path(s)>] [OPTIONS]
```

### [OPTIONS]

Note: One or both of `--status_changes` and `--trips` with accompanying path(s) to data files is **required**.

Additional options include:

```
--no_validate         Do not perform JSON Schema validation against the input file(s)

                      The default is to validate each input file and only load those
                      that pass validation.
```

### Configuration

The follow environment variables are **required** for this container to execute:

```bash
POSTGRES_HOSTNAME=server
MDS_DB=mds_provider
MDS_USER=mds_provider
MDS_PASSWORD=mds_provider_password
```

## Local Development

The container makes available a Jupyter Notebook server to the host at http://localhost:$NB_LOAD_HOST_PORT.

This directory is the root of the Notebook server filesystem.

First, ensure the image is up to date locally:

```bash
$ docker-compose build --no-cache load
```

Then start the notebook server with:

```bash
$ docker-compose run --service-ports --entrypoint bash load start-notebook.sh
```

**Note** the additional parameters given to the `docker-compose run` command:

  - `--service-ports` ensures http://localhost:$NB_LOAD_HOST_PORT is mapped correctly to the notebook server

  - `--entrypoint bash` overrides the container's entrypoint from running the data generation script to running bash

  - `start-notebook.sh` is a script from the base container that starts the Jupyter Notebook server, execucted by the bash entrypoint

### Configuration

Configure the notebook environment using the following environment variables:

```bash
NB_USER=joyvan
NB_UID=1000
NB_GID=100
NB_LOAD_HOST_PORT=8889
```