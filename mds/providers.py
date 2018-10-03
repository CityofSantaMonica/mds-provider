import csv
import mds
import requests
import uuid


PROVIDER_REGISTRY = "https://raw.githubusercontent.com/CityOfLosAngeles/mobility-data-specification/{}/providers.csv"


class Provider():
    """
    A simple model for an entry in the Provider registry.
    """
    def __init__(self, **kwargs):
        self.name = kwargs["provider_name"]
        self.id = uuid.UUID(kwargs["provider_id"])
        self.url = kwargs["url"]
        self.mds_api_url = kwargs["mds_api_url"]


def get_registry(branch="master"):
    """
    Download and parse the current Provider registry.

    Optionally download from a specified :branch:. The default is `master`.
    """
    providers = []
    url = PROVIDER_REGISTRY.format(branch)

    with requests.get(url, stream=True) as r:
        lines = (line.decode("utf-8") for line in r.iter_lines())
        for record in csv.DictReader(lines):
            providers.append(Provider(**record))

    return providers

