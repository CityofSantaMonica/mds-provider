"""
Work with the MDS Provider JSON Schemas.
"""

import json
import jsonschema
import mds
import os
import requests
import urllib


class ProviderSchema():
    """
    Class for acquiring and working with an MDS Provider JSON Schema.
    """
    SCHEMA_ROOT = "https://raw.githubusercontent.com/CityOfLosAngeles/mobility-data-specification/{}/provider/{}.json"
    DEFAULT_REF = "master"

    @classmethod
    def StatusChanges(cls, ref=DEFAULT_REF):
        """
        Acquires the Status Changes schema.
        """
        return ProviderSchema(mds.STATUS_CHANGES, ref=ref)

    @classmethod
    def Trips(cls, ref=DEFAULT_REF):
        """
        Acquires the Trips schema.
        """
        return ProviderSchema(mds.TRIPS, ref=ref)

    def __init__(self, schema_type, ref=DEFAULT_REF):
        """
        Initialize a new ProviderSchema of the given :schema_type:.

        :ref: optionally checks the schema at the version specified, which could be any of:
            - git branch name
            - commit hash (long or short)
            - git tag
        """
        if schema_type not in [mds.STATUS_CHANGES, mds.TRIPS]:
            raise ValueError("Invalid schema type '{}'".format(schema_type))

        # acquire the schema
        schema_url = self.SCHEMA_ROOT.format(ref, schema_type)
        self.schema = requests.get(schema_url).json()
        self.schema_type = schema_type

    def event_types(self):
        """
        Get the list of valid event types for this schema.
        """
        return list(self.event_type_reasons().keys())

    def event_type_reasons(self):
        """
        Get a dictionary of event_type => event_type_reasons for this schema.
        """
        etr = {}
        if self.schema_type is not mds.STATUS_CHANGES:
            return etr

        item_schema = self.item_schema()
        for oneOf in item_schema["oneOf"]:
            props = oneOf["properties"]
            if "event_type" in props and "event_type_reason" in props:
                event_type = props["event_type"]["enum"][0]
                event_type_reasons = props["event_type_reason"]["enum"]
                etr[event_type] = event_type_reasons

        return etr

    def item_schema(self):
        """
        Get the schema for items in this schema's data array.

        e.g. the schema for the actual record this schema represents (Status Changes or Trips)
        without all of the metadata envelope.
        """
        return self.schema["properties"]["data"]["properties"][self.schema_type]["items"]

    def optional_item_fields(self):
        """
        Returns the list of optional field names for items in the data array of this schema.
        """
        item_schema = self.item_schema()
        item_required = item_schema["required"]
        item_props = item_schema["properties"].keys()
        return [ip for ip in item_props if ip not in item_required]

    def required_item_fields(self):
        """
        Returns the list of required field names for items in the data array of this schema.
        """
        item_schema = self.item_schema()
        return item_schema["required"]

    def propulsion_types(self):
        """
        Get the list of valid propulsion type values for this schema.
        """
        definition = self.schema["definitions"]["propulsion_type"]
        return definition["items"]["enum"]

    def vehicle_types(self):
        """
        Get the list of valid propulsion type values for this schema.
        """
        definition = self.schema["definitions"]["vehicle_type"]
        return definition["items"]["enum"]

    def validate(self, instance_source):
        """
        Validate the given :instance_source: against this schema.

        :instance_source: can be any of:
            - JSON text (e.g. str)
            - JSON object (e.g. dict)
            - path to a file with JSON text
            - URL to a file of JSON text

        Invalid instances raise one or more Exceptions. Valid instances validate silently.
        """
        def __isurl(check):
            """
            Return True if :check: is a valid URL, False otherwise.
            """
            parts = urllib.parse.urlparse(check)
            return parts.scheme and parts.netloc

        # and the instance
        if isinstance(instance_source, str):
            if os.path.isfile(instance_source):
                instance = json.load(open(instance_source, "r"))
            elif __isurl(instance_source):
                instance = requests.get(instance_source).json()
            else:
                instance = json.loads(instance_source)
        elif isinstance(instance_source, dict):
            instance = instance_source
        else:
            raise TypeError("Unrecognized :instance_source: format. Recognized formats: file path/URL, JSON string, dict")

        # do validation, raising Exceptions for invalid schemas
        jsonschema.validate(instance, self.schema)

