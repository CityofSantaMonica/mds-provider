#!/bin/bash

echo "[server] is running"

# copy the environment file
cp ../.env.sample ./.env

# pull an image preconfigured for postgreSQL and postGIS
docker pull mdillon/postgis

# remove any existing instance of this container
docker rm -v --force mds_provider_store_postgres_server

# run a new instance of the pulled image
# configured through the environment variables in .env
docker run --publish "5432:5432" \
  --name "mds_provider_store_postgres_server" \
  --env-file ".env" \
  --detach \
mdillon/postgis

rm --force .env
