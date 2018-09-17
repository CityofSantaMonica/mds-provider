# fake

Generate fake MDS `provider` data for testing and development.

## Running

Run the container to generate randomized data, destroying the container when finished.
The data is persisted in this directory in `./data` by default, via a Docker volume.

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

--start         YYYY-MM-DD of the earliest event in the generated data

--end           YYYY-MM-DD of the latest event in the generated data

--open          The hour of the day (24-hr format) that provider begins operations

--close         The hour of the day (24-hr format) that provider stops operations

--inactivity    The percent of the fleet that remains inactive; e.g.
                --inactivity=0.05 means 5% of the fleet remains inactive

--output        Path to a directory (in the container) to write the resulting data file(s)
```

## Container Configuration

The container uses the following environment variables:

```bash
MDS_BOUNDARY=https://opendata.arcgis.com/datasets/bcc6c6245c5f46b68e043f6179bab153_3.geojson

NB_USER=joyvan
NB_UID=1000
NB_GID=100
NB_HOST_PORT=8888
```

### `$MDS_BOUNDARY`

Should be a path or URL to a `.geojson` file in [4326](http://epsg.io/4326), 
containing a FeatureCollection of (potentially overlapping) Polygons. See the file at the above URL for an example.

The subsequent generated data will be within the unioned area of these Polygons.

### Local Development

The container makes available a Jupyter Notebook server to the host at http://localhost:$NB_HOST_PORT
This directory is mounted under `./mds`.

Start the notebook server:

```bash
$ docker-compose run --rm fake bash "~/start-notebook.sh"
```

**Note** the wrapping quotes around the `~/start-notebook.sh` path: 
this is so `~/` is evaluated inside the container, and not in the host.