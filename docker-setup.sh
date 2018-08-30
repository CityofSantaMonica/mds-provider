#!/bin/bash

# creates a Data Volume container to persist the actual data
docker create -v "/var/lib/postgresql/data" \
    --name "mds-provider-store-postgres10.5-data" \
busybox

# builds an image from the Dockerfile in this repo, tagging it as
# mds-provider-store
docker build -t mds-provider-store .
