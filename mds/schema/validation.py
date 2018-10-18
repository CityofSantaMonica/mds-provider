"""
Validate instances of MDS Provider data against the schemas.
"""

import json
import jsonschema
import mds
from mds.json import extract_point
from mds.schema import ProviderSchema
import os
import requests
import urllib


class ProviderDataValidationError():
    """
    Represents a failed MDS Provider data validation.
    """

    def __init__(self, validation_error, instance, provider_schema):
        """
        Initialize a new validation error instance with:

            - :validation_error:, the original jsonschema.exceptions.ValidationError
            - :instance:, the MDS Provider data object under validation
            - :provider_schema:, the `ProviderSchema` instance used as the basis for validation
        """
        self.instance = validation_error.instance
        self.message = validation_error.message
        self.original_instance = instance
        self.path = list(validation_error.path)
        self.provider_schema = provider_schema
        self.schema_type = provider_schema.schema_type
        self.validation_error = validation_error
        self.validator = validation_error.validator

    def __repr__(self):
        return self.describe()

    def describe(self):
        """
        Describe this error.
        """
        if len(self.path) >= 4:
            messages = self._describe_item()
        elif len(self.path) >= 2:
            messages = self._describe_payload()
        else:
            messages = self._describe_page()

        # empty line to provide space between descriptions
        messages.append("")
        return os.linesep.join(messages)

    def _describe_page(self):
        """
        Describe a page-level error.
        """
        messages = [
            f"Page error"
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
        if "is valid under each of" in self.message:
            return []

        path = ".".join(self.path[:2])

        if len(self.path) > 2:
            path = f"{path}[{self.path[2]}]"

        message = self.message

        if "is not of type" in self.message:
            message = "value " + \
                self.message[self.message.index("is not of type"):]

        messages = [
            f"Payload error in {path}",
            message
        ]

        return messages

    def _describe_item(self):
        """
        Describe an item-level error.
        """
        index = self.path[2]
        field = self.path[3]
        item = self.original_instance["data"][self.schema_type][index]
        item_path = f"{self.schema_type}[{index}]"

        messages = [
            f"Item error in {item_path}.{field}: {self.message}",
            f"{item_path} snippet:",
        ]

        snippet = [
            "{",
            f"  'provider_name': '{item['provider_name']}',",
            f"  'device_id': '{item['device_id']}',",
            f"  'vehicle_id': '{item['vehicle_id']}',",
            f"  'vehicle_type': '{item['vehicle_type']}',",
            f"  'propulsion_type': {item['propulsion_type']},",
        ]

        if self.schema_type == mds.STATUS_CHANGES:
            snippet.extend([
                f"  'event_time': '{item['event_time']}',",
                f"  'event_location': '{extract_point(item['event_location'])}'"
            ])
        elif self.schema_type == mds.TRIPS:
            snippet.extend([
                f"  'trip_id': '{item['trip_id']}',",
                f"  'start_time': '{item['start_time']}',",
                f"  'end_time': '{item['end_time']}'",
            ])

        snippet.append("}")

        return messages + snippet


class ProviderDataValidator():
    """
    Validate MDS Provider data against JSON Schemas.
    """

    def __init__(self, provider_schema=None, schema_type=None, ref=None):
        """
        Initialize a new `ProviderSchemaValidator`.

        :provider_schema: is an optional `ProviderSchema` instance to use for later validation.

        If :schema_type: (and optionally :ref:) is given, obtain a new schema instance.
        """
        self.schema = self._get_schema_instance(
            provider_schema, schema_type, ref)

    def _get_schema_instance(self, provider_schema, schema_type, ref):
        """
        Helper to return a `ProviderSchema` instance from the possible arguments.
        """
        # determine the ProviderSchema instance to use
        if isinstance(provider_schema, mds.schema.ProviderSchema):
            return provider_schema
        elif schema_type:
            return mds.schema.ProviderSchema(schema_type, ref=ref)
        elif isinstance(self.schema, mds.schema.ProviderSchema):
            return self.schema
        else:
            return None

    def _get_validator(self, schema):
        """
        Helper to return a jsonschema.IValidator instance for the given JSON :schema: object.
        """
        return jsonschema.Draft6Validator(schema)

    def validate(self, instance_source, provider_schema=None, schema_type=None, ref=None):
        """
        Validate the given :instance_source:, which can be any of:
            - JSON text (e.g. str)
            - JSON object (e.g. dict)
            - path to a local file of JSON text
            - URL to a remote file of JSON text

        If :provider_schema: is given (a `ProviderSchema` instance), then validate against it.

        If :schema_type: (and optionally :ref:) is given, obtain a new schema instance and validate against it.

        Otherwise use the schema that this validator was initialized with.

        Yields a list of `ProviderDataValidationError`.
        """
        def __isurl(check):
            """
            Return True if :check: is a valid URL, False otherwise.
            """
            parts = urllib.parse.urlparse(check)
            return parts.scheme and parts.netloc

        # get the instance as a dict object
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
            raise TypeError(
                "Unrecognized :instance_source: format. Recognized formats: file path/URL, JSON string, dict")

        schema = self._get_schema_instance(provider_schema, schema_type, ref)
        if schema is None:
            raise ValueError(
                "Pass a valid ProviderSchema instance or a schema_type and ref to use for validation.")

        # schema is a ProviderSchema instance, schema.schema is the JSON Schema (dict) associated with it
        v = self._get_validator(schema.schema)

        # do validation, converting and yielding errors
        for error in v.iter_errors(instance):
            yield ProviderDataValidationError(error, instance, schema)

    @classmethod
    def StatusChanges(cls, ref=None):
        """
        Create a Status Changes validator.
        """
        return ProviderDataValidator(schema_type=mds.STATUS_CHANGES, ref=ref)

    @classmethod
    def Trips(cls, ref=None):
        """
        Create a Trips validator.
        """
        return ProviderDataValidator(schema_type=mds.TRIPS, ref=ref)
