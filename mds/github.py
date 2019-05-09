"""
Data and helpers for MDS on GitHub.
"""


GITHUB = "https://github.com"
GITHUB_RAW = "https://raw.githubusercontent.com"

MDS_DEFAULT_REF = "master"
MDS_ORG_NAME = "CityOfLosAngeles"
MDS_REPO_NAME = "mobility-data-specification"

MDS = (GITHUB, MDS_ORG_NAME, MDS_REPO_NAME)
MDS_RAW = (GITHUB_RAW, MDS_ORG_NAME, MDS_REPO_NAME)

MDS_PROVIDER_REGISTRY = "/".join(MDS_RAW + ("{}/providers.csv",))
MDS_SCHEMA = "/".join(MDS_RAW + ("{}/provider/{}.json",))


def registry_url(ref=None):
    """
    Helper to return a formatted provider registry URL.

    Parameters:
        ref: str, Version, optional
            Reference the schema at the version specified, which could be any of:
            * git branch name
            * git commit hash (long or short)
            * version str or Version instance

    Return:
        str
    """
    ref = ref or MDS_DEFAULT_REF
    return MDS_PROVIDER_REGISTRY.format(ref)


def schema_url(schema_type, ref=None):
    """
    Helper to return a formatted schema URL.

    Parameters:
        schema_type: str
                The type of MDS Provider schema ("status_changes" or "trips").

        ref: str, Version, optional
            Reference the schema at the version specified, which could be any of:
            * git branch name
            * git commit hash (long or short)
            * version str or Version instance

    Return:
        str
    """
    ref = ref or MDS_DEFAULT_REF
    return MDS_SCHEMA.format(ref, schema_type)
