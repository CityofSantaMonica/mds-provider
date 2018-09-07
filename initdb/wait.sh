#!/bin/sh
set -e

cmd="$@"

until PGPASSWORD=$POSTGRES_PASSWORD PGUSER=$POSTGRES_USER \
      psql -h "$POSTGRES_HOSTNAME" -c '\q'; do
  >&2 echo "Waiting for $POSTGRES_HOSTNAME"
  sleep 5
done

>&2 echo "$POSTGRES_HOSTNAME is available"

exec $cmd
