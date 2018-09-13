# fake

Generate fake MDS `provider` data for testing and development.

## Configuration

The container uses the following environment variables:

```bash
MDS_BOUNDARY=https://opendata.arcgis.com/datasets/bcc6c6245c5f46b68e043f6179bab153_3.geojson

NB_USER=joyvan
NB_UID=1000
NB_GID=100
NB_HOST_PORT=8888
```

### `$MDS_BOUNDARY`

This should be the path or URL to a `.geojson` file in [4326](http://epsg.io/4326), containing a FeatureCollection of (potentially overlapping) Polygons. See the file at the above URL for an example.

The subsequent generated data will be within the unioned area of these Polygons.

### Local Development

The container makes available a Jupyter Notebook server to the host at http://localhost:$NB_HOST_PORT, where this directory is mounted under `./mds`.