"""
Work with the MDS Provider JSON Schemas.
"""

import json
import jsonschema
import mds
import requests


SCHEMA_ROOT = "https://raw.githubusercontent.com/CityOfLosAngeles/mobility-data-specification/{}/provider/{}.json"
DEFAULT_REF = "master"

def get_schema(schema_type, ref=DEFAULT_REF):
    """
    Gets the schema of type :schema_type: at :ref: or the default (`master`).
    """
    if schema_type not in [mds.STATUS_CHANGES, mds.TRIPS]:
        raise ValueError("Invalid schema type '{}'".format(schema_type))

    # acquire the schema
    schema_url = SCHEMA_ROOT.format(ref, schema_type)
    return requests.get(schema_url).json()

def get_status_changes_schema(ref=DEFAULT_REF):
    """
    Gets the Status Changes schema at :ref: or the default (`master`).
    """
    return get_schema(mds.STATUS_CHANGES, ref)

def get_trips_schema(ref=DEFAULT_REF):
    """
    Gets the Trips schema at :ref: or the default (`master`).
    """
    return get_schema(mds.TRIPS, ref)

def get_optional_fields(schema_type, ref=DEFAULT_REF):
    """
    Returns the list of optional field names for the given :schema_type: at the given :ref:.
    """
    schema = get_schema(schema_type, ref=ref)
    item_schema = schema["properties"]["data"]["properties"][schema_type]["items"]
    item_required = item_schema["required"]
    item_props = item_schema["properties"].keys()
    return [ip for ip in item_props if ip not in item_required]

def get_required_fields(schema_type, ref=DEFAULT_REF):
    schema = get_schema(schema_type, ref=ref)
    item_schema = schema["properties"]["data"]["properties"][schema_type]["items"]
    return item_schema["required"]

