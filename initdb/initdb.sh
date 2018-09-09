#!/bin/sh
set -e

export PGPASSWORD=$POSTGRES_PASSWORD
export PGUSER=$POSTGRES_USER

# run the setup scripts
psql \
    --host "$POSTGRES_HOSTNAME" \
    --dbname "$POSTGRES_DB" \
    --file scripts/common.sql \
    --file scripts/trips.sql \
    --file scripts/status_changes.sql
