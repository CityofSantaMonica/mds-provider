# fake

Generate fake MDS `provider` data for testing and development.

## Running

Run the container to generate randomized data, destroying the container when finished.

The data is persisted in this directory in a `data/` subdirectory by default, via a Docker volume.

```bash
$ docker-compose run --rm fake
```

### Options

First change the above `run` command to:

```bash
$ docker-compose run --rm fake python main.py [OPTIONS]
```

Then customize data generation by appending any combination of the following OPTIONS
like `--option1 value1 --option2 value2`:

```
--boundary      Path (within the container) or URL to a .geojson file
                with geographic bounds for the generated data.

                Overrides the $MDS_BOUNDARY environment variable.

--provider      The name of the fake mobility as a service provider

--devices       The number of devices to model in the generated data

--date_format   Format for datetime I/O. Options:
                    - 'unix' for Unix timestamps (default)
                    - 'iso8601' for ISO 8601 format
                    - '<python format string>' for custom formats, see
                       https://docs.python.org/3/library/datetime.html#strftime-strptime-behavior

--start         The earliest event in the generated data, in --date_format format

--end           The latest event in the generated data, in --date_format format

--open          The hour of the day (24-hr format) that provider begins operations. 
                Overrides --start and --end.

--close         The hour of the day (24-hr format) that provider stops operations.
                Overrides --start and --end.

--inactivity    Describes the portion of the fleet that remains inactive; e.g.
                --inactivity=0.05 means 5 percent of the fleet remains inactive

--speed_mph     The average speed of devices in miles per hour. Overridden by --speed_ms.

--speed_ms      The average speed of devices in meters per second. Overrides --speed_mph.

--output        Path to a directory (in the container) to write the resulting data file(s)
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
$ docker-compose run --rm fake start-notebook.sh
```

`--rm` cleans up the container and its resources when it shuts down.

### Configuration

Configure the development environment using the following environment variables:

```bash
NB_USER=joyvan
NB_UID=1000
NB_GID=100
NB_HOST_PORT=8888
```
