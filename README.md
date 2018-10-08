# mds-provider

Tools for working with [MDS `provider`][provider] data.

Developed and tested with Python 3.7+.

## Installation

Install with `pip`:

```bash
$ pip install -e git+https://github.com/CityofSantaMonica/mds-provider@master#egg=mds_provider
```

Or with `python` directly:

```
$ git clone https://github.com/CityofSantaMonica/mds-provider.git
$ cd mds-provider
$ python setup.py install
```

## Package organization

| module | description |
| --------- | ----------- |
| [`mds`](mds/__init__.py) | Tools for working with Mobility Data Specification `provider` data |
| [`api`](mds/api/) | Request `provider` data from compatible API endpoints |
| [`db`](mds/db/) | Load `provider` data into a database |
| [`fake`](mds/fake/) | Generate fake `provider` data for testing and development |
| [`json`](mds/json.py) | Work with `provider` data as (Geo)JSON files and objects |
| [`providers`](mds/providers.py) | Work with the official [MDS Providers registry][registry] |
| [`validate`](mds/validate.py) | Validate `provider` data against the official [JSON Schema][schema] |

[provider]: https://github.com/CityOfLosAngeles/mobility-data-specification/tree/master/provider
[registry]: https://github.com/CityofLosAngeles/mobility-data-specification/blob/master/providers.csv
[schema]: https://github.com/CityOfLosAngeles/mobility-data-specification/tree/master/generate_schema