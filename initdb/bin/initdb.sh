#!/bin/sh
set -e

# create the MDS user and database

export PGUSER=$POSTGRES_USER
export PGPASSWORD=$POSTGRES_PASSWORD

psql -v ON_ERROR_STOP=1 --host "$POSTGRES_HOSTNAME" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER $MDS_USER WITH PASSWORD '$MDS_PASSWORD';

    CREATE DATABASE $MDS_DB
        WITH OWNER $MDS_USER
        TEMPLATE 'template_postgis'
        ENCODING 'UTF8'
        CONNECTION LIMIT -1
    ;
EOSQL

# run the MDS setup scripts

export PGUSER=$MDS_USER
export PGPASSWORD=$MDS_PASSWORD

psql \
    --host "$POSTGRES_HOSTNAME" \
    --dbname "$MDS_DB" \
    --file scripts/common.sql \
    --file scripts/trips.sql \
    --file scripts/status_changes.sql
