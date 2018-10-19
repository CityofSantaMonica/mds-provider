 The [`mds.schema.validation`](https://github.com/CityofSantaMonica/mds-provider/blob/master/mds/schema/validation.py) module supports validating MDS Provider data against the published [MDS JSON Schema](https://github.com/CityOfLosAngeles/mobility-data-specification/tree/master/generate_schema) documents.

This module exports the `ProviderDataValidator` class, which works hand-in-hand with the [[ProviderSchema|Schemas: Introspection]] class.

## Example Usage

The easiest way is to create an instance of a validator for a schema type and (git) reference, and then run validation against some input data:

```python
from mds.schema.validation import ProviderDataValidator

validator = ProviderDataValidator.StatusChanges(ref="dev")
data = {}

for error in validator.validate(data):
    print(error)
```

Outputs a `Page error`:

```console
Page error
'version' is a required property

Page error
'data' is a required property
```

Indicating a problem with the top-level data structure (the page of data).

With slightly more complex data:

```python
data = { "version": "0.2.0", "data": { "status_changes": "data here" } }

for error in validator.validate(data):
    print(error)
```

Outputs a `Payload error`:

```console
Payload error in data.status_changes
value is not of type 'array'
```

Indicating a problem with the `data` payload (in this case, `"data here"` is not the required type `array`)

A more involved `Payload error`:

```
data = { "version": "0.2.0", "data": { "status_changes": [{}] } }

for error in validator.validate(data):
    print(error)
```

Outputs:

```console
Payload error in data.status_changes[0]
'provider_name' is a required property

Payload error in data.status_changes[0]
'provider_id' is a required property

Payload error in data.status_changes[0]
'device_id' is a required property

Payload error in data.status_changes[0]
'vehicle_id' is a required property

Payload error in data.status_changes[0]
'vehicle_type' is a required property

Payload error in data.status_changes[0]
'propulsion_type' is a required property

Payload error in data.status_changes[0]
'event_type' is a required property

Payload error in data.status_changes[0]
'event_type_reason' is a required property

Payload error in data.status_changes[0]
'event_time' is a required property

Payload error in data.status_changes[0]
'event_location' is a required property
```

If you already have an instance of `ProviderSchema`, this can be used to initialize a validator:

```python
from mds.schema import ProviderSchema
from mds.schema.validation import ProviderDataValidator

schema = ProviderSchema.StatusChanges(ref="dev")
validator = ProviderDataValidator(schema)
data = {}

for error in validator.validate(data):
    print(error)
```

A shortcut for the above is to simply call `validate()` on the `ProviderSchema` instance itself:

```python
from mds.schema import ProviderSchema
from mds.schema.validation import ProviderDataValidator

schema = ProviderSchema.StatusChanges(ref="dev")
data = {}

for error in schema.validate(data):
    print(error)
```
