"""
Work with the MDS Provider JSON Schemas.
"""

import os

import jsonschema
import requests

import mds.geometry
import mds.github
from .versions import Version


STATUS_CHANGES = "status_changes"
TRIPS = "trips"
EVENTS = "events"
VEHICLES = "vehicles"
SCHEMA_TYPES = [ STATUS_CHANGES, TRIPS, EVENTS, VEHICLES ]


class Schema():
    """
    Represents a MDS Provider JSON Schema.
    """

    def __init__(self, schema_type, ref=None):
        """
        Initialize a new Schema instance.

        Parameters:
            schema_type: str
                The type of MDS Provider schema.

            ref: str, Version, optional
                Reference the schema at the version specified, which could be any of:
                * git branch name
                * git commit hash (long or short)
                * version str or Version instance
        """
        if schema_type not in SCHEMA_TYPES:
            valid_types = ", ".join(SCHEMA_TYPES)
            raise ValueError(f"Invalid schema_type '{schema_type}'. Valid schema_types: {valid_types}")

        # acquire the schema
        self.schema_type = schema_type
        self.schema_key = STATUS_CHANGES if schema_type == EVENTS else schema_type
        self.ref = ref or mds.github.MDS_DEFAULT_REF

        try:
            self.ref = Version(self.ref)
        except:
            pass
        finally:
            if isinstance(self.ref, Version):
                self.ref.raise_if_unsupported()

        self.schema_url = mds.github.schema_url(schema_type, self.ref)

        try:
            self.schema = requests.get(self.schema_url).json()
        except:
            raise ValueError(f"Problem requesting schema from: {self.schema_url}")

        # override the $id for a non-standard ref
        if self.ref != mds.github.MDS_DEFAULT_REF:
            self.schema["$id"] = self.schema_url

    def __repr__(self):
        return f"<mds.schemas.Schema ('{self.schema_type}', '{self.ref}', '{self.schema_url}')>"

    def validate(self, instance_source):
        """
        Validate an instance against this schema.

        Shortcut method for DataValidator(self).validate(instance_source).

        Parameters:
            instance_source: dict
                An instance (e.g. parsed JSON object) to validate.

        Return:
            iterator
                An iterator that yields validation errors.
        """
        validator = DataValidator(self)
        for error in validator.validate(instance_source):
            yield error

    @property
    def event_types(self):
        """
        Get the list of valid event_type values for this schema.
        """
        return list(self.event_type_reasons.keys())

    @property
    def event_type_reasons(self):
        """
        Get a dict(event_type=list(event_type_reason)) for this schema.
        """
        etr = {}
        if self.schema_key != STATUS_CHANGES:
            return etr

        if "allOf" in self.item_schema:
            for allOf in self.item_schema["allOf"]:
                sub_check = ["properties" in sub and "event_type" in sub["properties"] for sub in allOf["oneOf"]]
                if all(sub_check):
                    item_schema = allOf["oneOf"]
                    break
        else:
            item_schema = self.item_schema["oneOf"]

        for oneOf in item_schema:
            props = oneOf["properties"]
            if "event_type" in props and "event_type_reason" in props:
                event_type = props["event_type"]["enum"][0]
                event_type_reasons = props["event_type_reason"]["enum"]
                etr[event_type] = event_type_reasons

        return etr

    @property
    def item_schema(self):
        """
        Get the schema for items in this schema's data array (e.g. the status_change or trip records).
        """
        return self.schema["properties"]["data"]["properties"][self.schema_key]["items"]

    @property
    def optional_item_fields(self):
        """
        Returns the list of optional field names for items in the data array of this schema.
        """
        item_props = self.item_schema["properties"].keys()
        return [ip for ip in item_props if ip not in self.required_item_fields]

    @property
    def required_item_fields(self):
        """
        Returns the list of required field names for items in the data array of this schema.
        """
        return self.item_schema["required"]

    @property
    def propulsion_types(self):
        """
        Get the list of valid propulsion_type values for this schema.
        """
        definition = self.schema["definitions"]["propulsion_type"]
        return definition["items"]["enum"]

    @property
    def vehicle_types(self):
        """
        Get the list of valid vehicle_type values for this schema.
        """
        definition = self.schema["definitions"]["vehicle_type"]
        return definition["enum"]

    @classmethod
    def status_changes(cls, ref=None):
        """
        Acquires the Status Changes schema.
        """
        return Schema(STATUS_CHANGES, ref)

    @classmethod
    def trips(cls, ref=None):
        """
        Acquires the Trips schema.
        """
        return Schema(TRIPS, ref)

    @classmethod
    def events(cls, ref=None):
        """
        Acquires the Events schema.
        """
        return Schema(EVENTS, ref)

    @classmethod
    def vehicles(cls, ref=None):
        """
        Acquires the Vehicles schema.
        """
        return Schema(VEHICLES, ref)


class DataValidationError():
    """
    Represents a failed MDS Provider data validation.
    """

    def __init__(self, validation_error, instance, provider_schema):
        """
        Initialize a new validation error instance.

        Parameters:
            validation_error: jsonschema.exceptions.ValidationError
                The error raised by validation.

            instance: dict
                The MDS Provider data object under validation.

            provider_schema: Schema
                The schema instance used as the basis for validation.
        """
        self.instance = validation_error.instance
        self.message = validation_error.message
        self.original_instance = instance
        self.version = Version(instance["version"])
        self.path = list(validation_error.path)
        self.provider_schema = provider_schema
        self.schema_type = provider_schema.schema_type
        self.schema_key = provider_schema.schema_key
        self.validation_error = validation_error
        self.validator = validation_error.validator

    def __repr__(self):
        return os.linesep.join(self.describe())

    def describe(self):
        """
        Describe this error.

        Return:
            list
                A list of error messages describing the error.
        """
        if len(self.path) >= 3:
            return self._describe_item()
        elif len(self.path) == 2:
            return self._describe_payload()
        else:
            return self._describe_page()

    def _describe_page(self):
        """
        Describe a page-level error.
        """
        messages = [
            "Page error"
        ]

        if len(self.path) > 0:
            for key in self.path:
                messages.append(f"Field '{key}': value {self.message}")
        else:
            messages.append(self.message)

        return messages

    def _describe_payload(self):
        """
        Describe a payload-level error.
        """
        path = ".".join(self.path)

        return [
            f"Payload error in {path}",
            self.message
        ]

    def _describe_item(self):
        """
        Describe an item-level error.
        """
        index = list(filter(lambda i: isinstance(i, int), self.path))[0]
        path = f"{self.schema_key}[{index}]"

        message = self.message.lower()
        if "is valid under each of" in message:
            message = "instance " + self.message[message.index("is valid under each of"):]
        if "is not of type" in message:
            message = "value " + self.message[message.index("is not of type"):]

        # this is an error about a specific attribute on this item
        if len(self.path) > 3:
            path = ".".join([path, self.path[-1]])

        return [
            f"Item error in {path}",
            message
        ]


class DataValidator():
    """
    Validate MDS Provider data against JSON Schemas.
    """

    def __init__(self, schema=None, ref=None):
        """
        Initialize a new DataValidator.

        Parameters:
            schema: str, Schema, optional
                The type of schema to validate; or
                A Schema instance to use for validation.

            ref: str, Version, optional
                The reference (git commit, branch, tag, or version) at which to reference the schema.
        """
        self.schema = self._get_schema_instance_or_raise(schema, ref)
        self.ref = self.schema.ref
        self.schema_type = self.schema.schema_type
        self.schema_key = self.schema.schema_key

    def __repr__(self):
        return f"<mds.schemas.DataValidator ('{self.ref}', '{self.schema_type}')>"

    def _get_schema_instance_or_raise(self, schema, ref):
        """
        Helper to return a Schema instance from the possible arguments.
        """
        # determine the Schema instance to use
        if isinstance(schema, Schema):
            return schema
        elif schema in SCHEMA_TYPES:
            return Schema(schema, ref=ref)
        elif isinstance(getattr(self, "schema", None), Schema):
            return self.schema
        else:
            raise ValueError("Could not obtain a schema for validation.")

    def validate(self, instance_source, schema=None, ref=None):
        """
        Validate MDS Provider data against a schema.

        Parameters:
            instance_source: str, dict, Path
                The source of data to validate, any of:
                * JSON text str
                * JSON object dict
                * path to a local file of JSON text
                * URL to a remote file of JSON text

            schema: str, Schema, optional
                The type of schema to validate; or
                A Schema instance to use for validation.

            ref: str, Version, optional
                The reference (git commit, branch, tag, or version) at which to reference the schema.

        Return:
            iterator
                Zero or more DataValidationError instances.
        """
        schema = self._get_schema_instance_or_raise(schema, ref)

        if isinstance(instance_source, dict):
            instances = [instance_source]
        else:
            try:
                from .files import DataFile
                instances = DataFile(schema, instance_source).load_payloads()
            except:
                raise TypeError(f"Unrecognized instance_source type: {type(instance_source)}.")

        # schema is a Schema instance
        # schema.schema is the JSON Schema (dict) associated with it
        v = self._get_validator(schema.schema)

        # handles case when instance_source pointed to a list of payloads
        for instance in instances:
            # do validation, converting and yielding errors
            for error in v.iter_errors(instance):
                yield DataValidationError(error, instance, schema)

    @classmethod
    def _get_validator(cls, schema):
        """
        Helper to return a jsonschema.IValidator instance for the given JSON schema object.
        """
        return jsonschema.Draft6Validator(schema)

    @classmethod
    def status_changes(cls, ref=None):
        """
        Create a Status Changes validator.
        """
        return DataValidator(STATUS_CHANGES, ref)

    @classmethod
    def trips(cls, ref=None):
        """
        Create a Trips validator.
        """
        return DataValidator(TRIPS, ref)

    @classmethod
    def events(cls, ref=None):
        """
        Create an Events validator.
        """
        return DataValidator(EVENTS, ref)

    @classmethod
    def vehicles(cls, ref=None):
        """
        Create a Vehicles validator.
        """
        return DataValidator(VEHICLES, ref)
