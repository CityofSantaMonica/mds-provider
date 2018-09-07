# client

A web client interface into `provider` data store server(s).

## Getting Started

Run these commands from a shell inside this directory.

### Build the `client` image

```bash
$ docker build --no-cache -t mds_provider_client .
```

  - `docker build .` builds according to the Dockerfile in this directory
  - `--no-cache` forces a rebuild of all steps
  - `-t mds_provider_client` tags this image with a name

### Create the `.env` file

```bash
$ cp ../.env.sample .env
```

### Run the `client` image

```bash
$ docker run \
    --name "mds_provider_client" \
    --env-file ".env" \
    --publish "8088:80" \
    --detach \
mds_provider_client
````

  - `docker run mds_provider_client` runs the previously built image
  - `--name "mds_provider_client"` give this container a name
  - `--env-file ".env"` use the indicated environment variables file. See [`.env.sample`](../.env.sample) for an example.
  - `--publish "8088:80"` maps port `8088` in the host to port `80` in the container.
  - `--detach` runs this container in the background

Now launch a browser in the host pointed at `localhost:8088`.