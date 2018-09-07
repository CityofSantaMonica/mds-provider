#!/bin/bash

# this needs to run inside a container distinct from the postgres container
# (since in prod, we don't have a postgres container, but RDS)

# run:
#  - init.sql
#  - common.sql
#  - trips.sql
#  - service_changes.sql

echo "[init] is running"
