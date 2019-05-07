"""
Validate instances of MDS Provider data against the schemas.
"""

import json
import jsonschema
import os
from pathlib import Path
import requests
import urllib

from ..geometry import extract_point

from .schema import ProviderSchema, STATUS_CHANGES, TRIPS


class ProviderDataValidationError():
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
                
            provider_schema: ProviderSchema
                The schema instance used as the basis for validation.
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

        if self.schema_type == STATUS_CHANGES:
            snippet.extend([
                f"  'event_time': '{item['event_time']}',",
                f"  'event_location': '{extract_point(item['event_location'])}'"
            ])
        elif self.schema_type == TRIPS:
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
        Initialize a new ProviderSchemaValidator.
        
        Parameters:
            provider_schema: ProviderSchema, optional
                A schema instance to use for later validation.
                
            schema_type: str, optional
                The type of schema to validate, e.g. "status_changes" or "trips"
                
            ref: str, Version, optional
                The reference (git commit, branch, tag, or version) at which to reference the schema.
        """
        self.schema = self._get_schema_instance(provider_schema, schema_type, ref)

    def _get_schema_instance(self, provider_schema, schema_type, ref):
        """
        Helper to return a ProviderSchema instance from the possible arguments.
        """
        # determine the ProviderSchema instance to use
        if isinstance(provider_schema, ProviderSchema):
            return provider_schema
        elif schema_type:
            return ProviderSchema(schema_type, ref=ref)
        elif isinstance(self.schema, ProviderSchema):
            return self.schema
        else:
            return None

    def _get_validator(self, schema):
        """
        Helper to return a jsonschema.IValidator instance for the given JSON schema object.
        """
        return jsonschema.Draft6Validator(schema)

    def validate(self, instance_source, provider_schema=None, schema_type=None, ref=None):
        """
        Validate MDS Provider data against a schema.
        
        Parameters:
            instance_source: str, dict, Path
                The aource of data to validate, any of:
                    - JSON text
                    - JSON object
                    - path to a local file of JSON text
                    - URL to a remote file of JSON text
            
            provider_schema: ProviderSchema, optional
                Schema instance against which to validate.
                
            schema_type: str, optional
                The type of schema to validate, e.g. "status_changes" or "trips"
                
            ref: str, Version, optional
                The reference (git commit, branch, tag, or version) at which to reference the schema.
        
        Returns:
            iterator
                Zero or more ProviderDataValidationError instances.
        """
        def _isurl(check):
            """
            Return True if check is a valid URL.
            """
            parts = urllib.parse.urlparse(check)
            return parts.scheme and parts.netloc

        # get the instance as a dict object
        if isinstance(instance_source, str):
            if os.path.isfile(instance_source):
                instance = json.load(open(instance_source, "r"))
            elif _isurl(instance_source):
                instance = requests.get(instance_source).json()
            else:
                instance = json.loads(instance_source)
        elif isinstance(instance_source, Path):
            instance = json.load(instance_source.open("r"))
        elif isinstance(instance_source, dict):
            instance = instance_source
        else:
            raise TypeError(f"Unrecognized instance_source type: {type(instance_source)}.")

        schema = self._get_schema_instance(provider_schema, schema_type, ref)
        if schema is None:
            raise ValueError("Could not obtain a schema for validation.")

        # schema is a ProviderSchema instance
        # schema.schema is the JSON Schema (dict) associated with it
        v = self._get_validator(schema.schema)

        # do validation, converting and yielding errors
        for error in v.iter_errors(instance):
            yield ProviderDataValidationError(error, instance, schema)

    @classmethod
    def status_changes(cls, ref=None):
        """
        Create a Status Changes validator.
        """
        return ProviderDataValidator(schema_type=STATUS_CHANGES, ref=ref)

    @classmethod
    def trips(cls, ref=None):
        """
        Create a Trips validator.
        """
        return ProviderDataValidator(schema_type=TRIPS, ref=ref)
