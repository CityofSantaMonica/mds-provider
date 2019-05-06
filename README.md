# mds-provider

Tools for working with [MDS `provider`][provider] data.

Developed and tested with Python 3.7+.

See [`mds-provider-services`](https://github.com/CityofSantaMonica/mds-provider-services) for real-world usage of many of these tools.

## Installation

Install with `pip`:

```bash
pip install -e git+https://github.com/CityofSantaMonica/mds-provider@master#egg=mds_provider
```

Or with `python` directly:

```bash
git clone https://github.com/CityofSantaMonica/mds-provider.git
cd mds-provider
python setup.py install
```

## Package organization

| module | description |
| --------- | ----------- |
| `mds`| Tools for working with Mobility Data Specification `provider` data |
| [`mds.api`](mds/api/) | Request `provider` data from compatible API endpoints |
| [`mds.db`](mds/db/) | Work with `provider` databases |
| [`mds.encoding`](mds/encoding.py) | Custom JSON encoding for `provider` data types |
| [`mds.fake`](mds/fake/) | Generate fake `provider` data for testing and development |
| [`mds.files`](mds/files.py) | Work with `provider` data in JSON files |
| [`mds.geometry`](mds/geometry.py) | Helpers for GeoJSON-based geometry objects |
| [`mds.providers`](mds/providers.py) | Parse [Providers registry][registry] files |
| [`mds.schemas`](mds/schemas/) | Validate data using [Provider JSON schemas][schemas] |
| [`mds.versions`](mds/versions.py) | Simple representation of [MDS versions][versions] |

[provider]: https://github.com/CityOfLosAngeles/mobility-data-specification/tree/master/provider
[registry]: https://github.com/CityofLosAngeles/mobility-data-specification/blob/master/providers.csv
[schemas]: https://github.com/CityOfLosAngeles/mobility-data-specification/tree/master/generate_schema
[versions]: https://github.com/CityofLosAngeles/mobility-data-specification/releases