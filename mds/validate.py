import json
import jsonschema
import os
import mds
import requests
import urllib


SCHEMA_ROOT = "https://raw.githubusercontent.com/CityOfLosAngeles/mobility-data-specification/master/provider/{}.json"


def isurl(check):
    """
    Return True if :check: is a valid URL, False otherwise.
    """
    parts = urllib.parse.urlparse(check)
    return parts.scheme and parts.netloc

def validate_schema_instance(instance_source, schema_type):
    """
    Validate the given :instance_source: (see notes below) against the current MDS Provider schema of the given :schema_type:.

    :instance_source: can be any of:
        - JSON text (e.g. str)
        - JSON object (e.g. dict)
        - path to a file with JSON text
        - URL to a file of JSON text

    :schema_type: can be one of:
        - status_changes
        - trips

    Invalid instances will raise one or more Exceptions. Valid instances validate silently.
    """
    if schema_type not in [mds.STATUS_CHANGES, mds.TRIPS]:
        raise ValueError("Invalid schema type '{}'".format(schema_type))

    # acquire the schema
    schema_file = SCHEMA_ROOT.format(schema_type)
    schema = requests.get(schema_file).json()

    # and the instance
    if isinstance(instance_source, str):
        if os.path.isfile(instance_source):
            instance = json.load(open(instance_source, "r"))
        elif isurl(instance_source):
            instance = requests.get(instance_source).json()
        else:
            instance = json.loads(instance_source)
    elif isinstance(instance_source, dict):
        instance = instance_source
    else:
        raise TypeError("Unrecognized :instance_source: format. Recognized formats: file path/URL, JSON string, dict")

    # do validation, raising Exceptions for invalid schemas
    jsonschema.validate(instance, schema)

def validate_status_changes(source):
    """
    Validate the given :source: (see notes below) against the current MDS Provider Status Changes schema.

    :source: can be any of:
        - JSON text (e.g. str)
        - JSON object (e.g. dict)
        - path to a file with JSON text
        - URL to a file of JSON text
    """
    validate_schema_instance(source, mds.STATUS_CHANGES)

def validate_trips(source):
    """
    Validate the given :source: (see notes below) against the current MDS Provider Trips schema.

    :source: can be any of:
        - JSON text (e.g. str)
        - JSON object (e.g. dict)
        - path to a file with JSON text
        - URL to a file of JSON text
    """
    validate_schema_instance(source, mds.TRIPS)

