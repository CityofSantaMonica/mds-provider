#!/bin/sh
set -e

# delete the MDS user and database

export PGUSER=$POSTGRES_USER
export PGPASSWORD=$POSTGRES_PASSWORD

psql -v ON_ERROR_STOP=1 --host "$POSTGRES_HOSTNAME" --dbname "$POSTGRES_DB" <<-EOSQL
    DROP DATABASE IF EXISTS $MDS_DB;
    DROP USER IF EXISTS $MDS_USER;
EOSQL

exec bin/initdb.sh
