"""
Validate MDS Provider data against the official JSON Schema.
"""

import json
import jsonschema
import os
import mds
import requests
import urllib


SCHEMA_ROOT = "https://raw.githubusercontent.com/CityOfLosAngeles/mobility-data-specification/{}/provider/{}.json"
DEFAULT_REF = "master"

def isurl(check):
    """
    Return True if :check: is a valid URL, False otherwise.
    """
    parts = urllib.parse.urlparse(check)
    return parts.scheme and parts.netloc

def validate_schema_instance(instance_source, schema_type, ref=DEFAULT_REF):
    """
    Validate the given :instance_source: against the current MDS Provider schema of the given :schema_type:.

    :instance_source: can be any of:
        - JSON text (e.g. str)
        - JSON object (e.g. dict)
        - path to a file with JSON text
        - URL to a file of JSON text

    :schema_type: can be one of:
        - status_changes
        - trips

    :ref: optionally checks the schema at the version specified, which could be any of:
        - git branch name
        - commit hash (long or short)
        - git tag

    Invalid instances raise one or more Exceptions. Valid instances validate silently.
    """
    if schema_type not in [mds.STATUS_CHANGES, mds.TRIPS]:
        raise ValueError("Invalid schema type '{}'".format(schema_type))

    # acquire the schema
    schema_file = SCHEMA_ROOT.format(ref, schema_type)
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

def validate_status_changes(source, ref=DEFAULT_REF):
    """
    Validate the given :instance_source: against the current MDS Provider schema of the given :schema_type:.

    :source: can be any of:
        - JSON text (e.g. str)
        - JSON object (e.g. dict)
        - path to a file with JSON text
        - URL to a file of JSON text

    :ref: optionally checks the schema at the version specified, which could be any of:
        - git branch name
        - commit hash (long or short)
        - git tag

    By default, check `master`.

    Invalid instances raise one or more Exceptions. Valid instances validate silently.
    """
    validate_schema_instance(source, mds.STATUS_CHANGES, ref)

def validate_trips(source, ref=DEFAULT_REF):
    """
    Validate the given :instance_source: against the current MDS Provider schema of the given :schema_type:.

    :source: can be any of:
        - JSON text (e.g. str)
        - JSON object (e.g. dict)
        - path to a file with JSON text
        - URL to a file of JSON text

    :ref: optionally checks the schema at the version specified, which could be any of:
        - git branch name
        - commit hash (long or short)
        - git tag

    By default, check `master`.
    """
    validate_schema_instance(source, mds.TRIPS, ref)

def validate_files(files, validator):
    """
    Runs the given schema :validator: function against all :files:.

    Returns a tuple:
        - a list of valid files
        - a map of invalid files => reported Exceptions
    """
    valid = []
    invalid = {}

    for f in files:
        try:
            validator(f)
            valid.append(f)
        except Exception as e:
            invalid[f] = e

    return valid, invalid

