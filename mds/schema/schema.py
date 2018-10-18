"""
Work with the MDS Provider JSON Schemas.
"""

import mds
import mds.schema
import requests


class ProviderSchema():
    """
    Represents a MDS Provider qJSON Schema.
    """
    SCHEMA_ROOT = "https://raw.githubusercontent.com/CityOfLosAngeles/mobility-data-specification/{}/provider/{}.json"
    DEFAULT_REF = "master"

    def __init__(self, schema_type, ref=DEFAULT_REF):
        """
        Initialize a new `ProviderSchema` of the given :schema_type:.

        :ref: optionally checks the schema at the version specified, which could be any of:
            - git branch name
            - commit hash (long or short)
            - git tag
        """
        if schema_type not in mds.schema.SCHEMA_TYPES:
            valid_types = ", ".join(mds.schema.SCHEMA_TYPES)
            raise ValueError(
                f"Invalid schema_type '{schema_type}'. Valid schema_types: {valid_types}")

        # acquire the schema
        schema_url = self.SCHEMA_ROOT.format(
            ref or self.DEFAULT_REF, schema_type)
        self.schema = requests.get(schema_url).json()
        self.schema_type = schema_type

    def event_types(self):
        """
        Get the list of valid `event_type` values for this schema.
        """
        return list(self.event_type_reasons().keys())

    def event_type_reasons(self):
        """
        Get a dict of `event_type` => `[event_type_reason]` for this schema.
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
        Get the schema for items in this schema's data array (e.g. the actual Status Change or Trip object).
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
        Get the list of valid `propulsion_type` values for this schema.
        """
        definition = self.schema["definitions"]["propulsion_type"]
        return definition["items"]["enum"]

    def vehicle_types(self):
        """
        Get the list of valid `vehicle_type` values for this schema.
        """
        definition = self.schema["definitions"]["vehicle_type"]
        return definition["enum"]

    def validate(self, instance_source):
        """
        Validate the given :instance_source: against this schema.

        Shortcut method for `ProviderSchemaValidator(self).validate(instance_source)`.
        """
        from mds.schema.validation import ProviderDataValidator
        validator = ProviderDataValidator(self)
        for error in validator.validate(instance_source):
            yield error

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
