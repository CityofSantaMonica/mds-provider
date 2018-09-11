#!/bin/sh
set -e

# wait for the postgres server to be available before executing cmd

cmd="$@"

until PGPASSWORD=$POSTGRES_PASSWORD PGUSER=$POSTGRES_USER \
      psql -h "$POSTGRES_HOSTNAME" -c '\q'; do
  >&2 echo "Waiting for $POSTGRES_HOSTNAME"
  sleep 1
done

>&2 echo "$POSTGRES_HOSTNAME is available"

exec $cmd
