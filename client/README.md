# client

A web client interface into `provider` data store server(s).

## Configuration

This container uses the following environment variables to configure pgAdmin4:

```bash
PGADMIN_DEFAULT_EMAIL=user@domain.com
PGADMIN_DEFAULT_PASSWORD=pgadmin_password
PGADMIN_HOST_PORT=8088
```

## Connecting

Once running, connect to the container from a web browser at:  
`http://localhost:$PGADMIN_HOST_PORT`.

Use the `$PGADMIN_DEFAULT_EMAIL` and `$PGADMIN_DEFAULT_PASSWORD` to log in.
