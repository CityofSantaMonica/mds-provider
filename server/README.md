# server

A local `provider` data store.

## Getting Started

Run these commands from a shell inside this directory.

### Build the `server` image

```bash
$ docker build --no-cache -t mds_provider_server .
```

  - `--no-cache` forces a rebuild of all steps
  - `-t mds_provider_server` tags this image with a name
  - `docker build .` builds according to the Dockerfile in this directory

### Run the `server` image

```bash
$ docker run \
    --name "mds_provider_server" \
    --env-file ".env" \
    --publish "5432:5432" \
    --detach \
mds_provider_server
````

  - `docker run mds_provider_server` runs the previously built image
  - `--name "mds_provider_server"` give this container a name
  - `--env-file ".env"` use the indicated environment variables file. See [`.env.sample`](.env.sample) for an example.
  - `--publish "5432:5432"` maps port `5432` in the host to port `5432` in the container.
  - `--detach` runs this container in the background

Connect to your local `server` using [`client`](../client)