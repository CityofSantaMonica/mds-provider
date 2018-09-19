# fake

Generate fake MDS `provider` data for testing and development.

## Running

Run the container to generate randomized data.

The data is persisted in this directory in a `data/` subdirectory
(even after the container is torn down), via a Docker volume.

**Note** `docker-compose` commands should be run from the root of this repository, where the 
`docker-compose.yml` file lives.

```bash
$ docker-compose run --rm fake [OPTIONS]
```

(`--rm` cleans up the container and its resources when it shuts down, and isn't strictly necessary)

### `[OPTIONS]`

Customize data generation by appending any combination of the following OPTIONS to the above command:

```
--boundary          Path (within the container) or URL to a .geojson file
                    with geographic bounds for the generated data.

                    Overrides the $MDS_BOUNDARY environment variable.

--close             The hour of the day (24-hr format) that provider stops operations.
                    Overrides --start and --end.

--date_format       Format for datetime input (to this CLI) and output (files and stdout)
                    Options:
                    - 'unix' for Unix timestamps (default)
                    - 'iso8601' for ISO 8601 format
                    - '<python format string>' for custom formats, see
                       https://docs.python.org/3/library/datetime.html#strftime-strptime-behavior

--devices           The number of devices to model in the generated data

--end               The latest event in the generated data, in --date_format format

--inactivity        Describes the portion of the fleet that remains inactive; e.g.
                    --inactivity=0.05 means 5 percent of the fleet remains inactive

--open              The hour of the day (24-hr format) that provider begins operations.
                    Overrides --start and --end.

--output            Path to a directory (in the container) to write the resulting data file(s)

--propulsion_types  A comma-separated list of propulsion_types to use for the generated data
                    e.g. --propulsion_types human,electric

--provider          The name of the fake mobility as a service provider

--speed_mph         The average speed of devices in miles per hour.
                    Overridden by --speed_ms.

--speed_ms          The average speed of devices in meters per second.
                    Overrides --speed_mph.

--start             The earliest event in the generated data, in --date_format format

--vehicle_types     A comma separated list of vehicle_types to use for the generated data
                    e.g. --vehicle_types scooter,bike
```

## Container Configuration

The container can use the following environment variables:

```bash
MDS_BOUNDARY=https://opendata.arcgis.com/datasets/bcc6c6245c5f46b68e043f6179bab153_3.geojson
```

### `$MDS_BOUNDARY`

Should be a path or URL to a `.geojson` file in [4326](http://epsg.io/4326), 
containing a FeatureCollection of (potentially overlapping) Polygons. See the file at the above URL for an example.

The subsequent generated data will be within the unioned area of these Polygons.

## Local Development

The container makes available a Jupyter Notebook server to the host at http://localhost:$NB_HOST_PORT

This directory is mounted under `./mds`.

First, ensure the image is up to date:

```bash
$ cd ./fake
$ docker build --no-cache -t mds_provider_fake .
```

Then start the notebook server:

```bash
$ cd ..
$ docker-compose run --rm --entrypoint bash fake start-notebook.sh
```

**Note** the additional `--entrypoint bash` option for the `docker-compose run` command.
This overrides the container's entrypoint from running the default data generation script `main.py`
to running the `start-notebook.sh` script from the base Docker image.

### Configuration

Configure the development environment using the following environment variables:

```bash
NB_USER=joyvan
NB_UID=1000
NB_GID=100
NB_HOST_PORT=8888
```
