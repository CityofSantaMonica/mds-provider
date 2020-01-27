"""
Data and helpers for MDS on GitHub.
"""
from .versions import Version


GITHUB = "https://github.com"
GITHUB_RAW = "https://raw.githubusercontent.com"

MDS_DEFAULT_REF = "master"
MDS_ORG_NAME = "openmobilityfoundation"
MDS_REPO_NAME = "mobility-data-specification"

MDS = (GITHUB, MDS_ORG_NAME, MDS_REPO_NAME)
MDS_RAW = (GITHUB_RAW, MDS_ORG_NAME, MDS_REPO_NAME)

MDS_PROVIDER_REGISTRY = "/".join(MDS_RAW + ("{}/providers.csv",))
MDS_OLD_SCHEMA = "/".join(MDS_RAW + ("{}/provider/{}.json",))
MDS_SCHEMA = "/".join(MDS_RAW + ("{}/provider/dockless/{}.json",))


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

    if is_pre_mds_040(ref):
        return MDS_OLD_SCHEMA.format(ref, schema_type)
    else:
        return MDS_SCHEMA.format(ref, schema_type)


def is_pre_mds_040(ref) -> bool:
    """
    Tries to determine if an unknown object is a string that represents
    MDS 0.2.x or 0.3.x. Reason being that the Open Mobility Foundation
    changed the URL structure of their repo with v0.4.0 by moving schemas
    to a 'dockless' folder.

    n.b. is not smart enough to determine if a hash is a reference to an older
    MDS version.

    Parameters:
        ref: Any
                Might be an MDS version. Will be coerced to a string.

    Return:
        bool
    """

    # if not a string representation of a version, e.g. 'master'
    if isinstance(ref, str) and len(ref.split('.')) < 2:
        return False

    if not isinstance(ref, Version):
        ref = Version(ref)

    try:
        if ref < Version('0.4.0'):
            return True

        return False
    except e:
        print(f"Unable to determine MDS version from '{ref}', assuming 0.4.0 or greater. Error: {e}")
        return False
