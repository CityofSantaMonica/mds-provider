import csv
import requests
import uuid


PROVIDER_REGISTRY = "https://raw.githubusercontent.com/CityOfLosAngeles/mobility-data-specification/{}/providers.csv"
DEFAULT_REF = "master"


class Provider():
    """
    A simple model for an entry in the Provider registry.
    """
    def __init__(self, **kwargs):
        self.provider_name = kwargs["provider_name"]
        self.provider_id = uuid.UUID(kwargs["provider_id"])
        self.url = kwargs["url"]
        self.mds_api_url = kwargs["mds_api_url"]

        for k,v in kwargs.items():
            setattr(self, k, v)

    def __repr__(self):
        return f"""<Provider name:{self.provider_name}
             id:{str(self.provider_id)}
             url:{self.url}
             api_url:{self.mds_api_url}>"""


def get_registry(ref=DEFAULT_REF):
    """
    Download and parse the official Provider registry from GitHub.

    Optionally download from the specified :ref:, which could be any of:
        - git branch name
        - commit hash (long or short)
        - git tag

    By default, downloads from `master`.
    """
    providers = []
    url = PROVIDER_REGISTRY.format(ref)

    with requests.get(url, stream=True) as r:
        lines = (line.decode("utf-8") for line in r.iter_lines())
        for record in csv.DictReader(lines):
            providers.append(Provider(**record))

    return providers

