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
| [`mds`](mds/__init__.py) | Tools for working with Mobility Data Specification `provider` data |
| [`api`](mds/api/) | Request `provider` data from compatible API endpoints |
| [`db`](mds/db/) | Load `provider` data into a database |
| [`fake`](mds/fake/) | Generate fake `provider` data for testing and development |
| [`json`](mds/json.py) | Work with `provider` data as (Geo)JSON files and objects |
| [`providers`](mds/providers.py) | Work with the official [MDS Providers registry][registry] |
| [`schema`](mds/schema/) | Work with the official [MDS Provider JSON schemas][schemas] |


[provider]: https://github.com/CityOfLosAngeles/mobility-data-specification/tree/master/provider
[registry]: https://github.com/CityofLosAngeles/mobility-data-specification/blob/master/providers.csv
[schemas]: https://github.com/CityOfLosAngeles/mobility-data-specification/tree/master/generate_schema