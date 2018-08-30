#!/bin/bash

# remove any existing instance of this container
docker rm -v --force mds-provider-store-postgres10.5

# run a new instance of the mds-provider-store image
# configured through the environment variables in pg.env
# persistence via a Data Volume container
docker run --publish "5432:5432" \
  --name "mds-provider-store-postgres10.5" \
  --env-file "pg.env" \
  --volumes-from "mds-provider-store-postgres10.5-data" \
  --detach \
mds-provider-store
