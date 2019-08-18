# mds-provider

Tools for working with [MDS `provider`][provider] data.

Developed and tested with Python 3.7+.

See [`mds-provider-services`](https://github.com/CityofSantaMonica/mds-provider-services) for real-world usage of many of these tools.

## Installation

Install with `pip`:

```bash
pip install -e git+https://github.com/CityofSantaMonica/mds-provider@master#egg=mds-provider
```

Or with `python` directly:

```bash
git clone https://github.com/CityofSantaMonica/mds-provider.git
cd mds-provider
python setup.py install
```

## Getting Started

### Read from a Provider API

```python
from datetime import datetime, timedelta

import mds

end = datetime.utcnow()
start = end - timedelta(hours=1)

client = mds.Client("provider_name", token="secret-token")

trips = client.get_trips(start_time=start, end_time=end)
```

### Validate against the MDS schema

```python
validator = mds.DataValidator.trips()

for error in validator.validate(trips):
    print(error)
```

### Load into a Postgres database

```python
db = mds.Database(user="user", password="password", host="host", db="database")

db.load_trips(trips)
```

## Package organization

| module | description |
| --------- | ----------- |
| `mds`| Tools for working with Mobility Data Specification `provider` data |
| [`mds.api`](mds/api/) | Request data from compatible API endpoints |
| [`mds.db`](mds/db/) | Work with databases |
| [`mds.encoding`](mds/encoding.py) | Custom data encoding and decoding. |
| [`mds.fake`](mds/fake/) | Generate fake `provider` data for testing and development |
| [`mds.files`](mds/files.py) | Work with `provider` configuration and data payload files |
| [`mds.geometry`](mds/geometry.py) | Helpers for GeoJSON-based geometry objects |
| [`mds.github`](mds/github.py) | Data and helpers for MDS on GitHub. |
| [`mds.providers`](mds/providers.py) | Parse [Provider registry][registry] files |
| [`mds.schemas`](mds/schemas.py) | Validate data using the [JSON schemas][schemas] |
| [`mds.versions`](mds/versions.py) | Work with [MDS versions][versions] |

[provider]: https://github.com/CityOfLosAngeles/mobility-data-specification/tree/master/provider
[registry]: https://github.com/CityofLosAngeles/mobility-data-specification/blob/master/providers.csv
[schemas]: https://github.com/CityOfLosAngeles/mobility-data-specification/tree/master/generate_schema
[versions]: https://github.com/CityofLosAngeles/mobility-data-specification/releases