"""
Generate SQL for MDS Provider database CRUD.
"""

from ..schemas import STATUS_CHANGES, TRIPS, VEHICLES
from ..versions import Version


_COMMON_INSERTS = [
    "provider_id",
    "provider_name",
    "device_id",
    "vehicle_id",
    "vehicle_type",
    "propulsion_type"
]

_COMMON_SELECTS = [
    "cast(provider_id as uuid)",
    "provider_name",
    "cast(device_id as uuid)",
    "vehicle_id",
    "cast(vehicle_type as vehicle_types)",
    "cast(propulsion_type as propulsion_types[])"
]


def insert_statement(dest_table, inserts, selects, source_table, on_conflict_update=None):
    """
    Generate an "INSERT INTO... SELECT... FROM..." statement.

    Parameters:
        dest_table: str
            The name of the table to INSERT INTO.

        inserts: list
            The list of column names to INSERT.

        selects: list
            The list of column names or expressions to SELECT.

        source_table: str
            The name of the table to SELECT FROM.

        on_conflict_update: tuple (condition: str, actions: str, list, dict), optional
            See on_conflict_statement().

    Return:
        str
    """
    inserts = ",".join(inserts)
    selects = ",".join(selects)

    on_conflict = on_conflict_statement(on_conflict_update)

    return f"""
    INSERT INTO "{dest_table}"
    ({inserts})
    SELECT {selects}
    FROM "{source_table}"
    {on_conflict}
    ;
    """


def on_conflict_statement(on_conflict_update=None):
    """
    Generate an appropriate "ON CONFLICT..." statement.

    Parameters:
        on_conflict_update: tuple (condition: str, actions: str, list, dict), optional
            Generate a statement like "ON CONFLICT condition DO UPDATE SET actions"
            actions may be a SQL str, a list of column action str, or a dict of column=value pairs.

    Return:
        str
    """
    if on_conflict_update:
        if isinstance(on_conflict_update, tuple):
            condition, actions = on_conflict_update
            if isinstance(actions, list):
                actions = ",".join(actions)
            elif isinstance(actions, dict):
                actions = ",".join([f"{k} = {v}" for k,v in actions.items()])
            return f"ON CONFLICT {condition} DO UPDATE SET {actions}"

    return "ON CONFLICT DO NOTHING"


def insert_status_changes_from(source_table, dest_table=STATUS_CHANGES, **kwargs):
    """
    Generate an "INSERT INTO... SELECT... FROM..." statement for status_changes.

    Parameters:
        source_table: str
            The name of the table to SELECT FROM.

        dest_table: str, optional
            The name of the table to INSERT INTO, by default status_changes.

        on_conflict_update: tuple (condition: str, actions: str, list, dict), optional
            See on_conflict_statement().

        version: str, Version, optional
            The MDS version to target. By default, Version.mds_lower().

    Raise:
        UnsupportedVersionError
            When an unsupported MDS version is specified.

    Return:
        str
    """
    version = Version(kwargs.get("version", Version.mds_lower()))
    version.raise_if_unsupported()

    on_conflict_update = kwargs.get("on_conflict_update")

    inserts = list(_COMMON_INSERTS)
    inserts.extend([
        "event_type",
        "event_type_reason",
        "event_location",
        "event_time",
        "battery_pct",
        "associated_trip",
        "publication_time"
    ])

    selects = list(_COMMON_SELECTS)
    selects.extend([
        "cast(event_type as event_types)",
        "cast(event_type_reason as event_type_reasons)",
        "cast(event_location as jsonb)",
        "to_timestamp(cast(event_time as double precision) / 1000.0) at time zone 'UTC'",
        "battery_pct",
        "cast(associated_trip as uuid)",
        "to_timestamp(cast(publication_time as double precision) / 1000.0) at time zone 'UTC'"
    ])

    if version >= Version._040_():
        inserts.append("associated_ticket")
        selects.append("associated_ticket")

    return insert_statement(dest_table, inserts, selects, source_table, on_conflict_update)


def insert_trips_from(source_table, dest_table=TRIPS, **kwargs):
    """
    Generate an "INSERT INTO... SELECT... FROM..." statement for trips.

    Parameters:
        source_table: str
            The name of the table to SELECT FROM.

        dest_table: str, optional
            The name of the table to INSERT INTO, by default trips.

        on_conflict_update: tuple (condition: str, actions: str, list, dict), optional
            See on_conflict_statement().

        version: str, Version, optional
            The MDS version to target. By default, Version.mds_lower().

    Raise:
        UnsupportedVersionError
            When an unsupported MDS version is specified.

    Return:
        str
    """
    version = Version(kwargs.get("version", Version.mds_lower()))
    version.raise_if_unsupported()

    on_conflict_update = kwargs.get("on_conflict_update")

    inserts = list(_COMMON_INSERTS)
    inserts.extend([
        "trip_id",
        "trip_duration",
        "trip_distance",
        "route",
        "accuracy",
        "start_time",
        "end_time",
        "parking_verification_url",
        "standard_cost",
        "actual_cost",
        "publication_time"
    ])

    selects = list(_COMMON_SELECTS)
    selects.extend([
        "cast(trip_id as uuid)",
        "trip_duration",
        "trip_distance",
        "cast(route as jsonb)",
        "accuracy",
        "to_timestamp(cast(start_time as double precision) / 1000.0) at time zone 'UTC'",
        "to_timestamp(cast(end_time as double precision) / 1000.0) at time zone 'UTC'",
        "parking_verification_url",
        "standard_cost",
        "actual_cost",
        "to_timestamp(cast(publication_time as double precision) / 1000.0) at time zone 'UTC'",
    ])

    if version >= Version._040_():
        inserts.append("currency")
        selects.append("currency")

    return insert_statement(dest_table, inserts, selects, source_table, on_conflict_update)


def insert_vehicles_from(source_table, dest_table=VEHICLES, **kwargs):
    """
    Generate an "INSERT INTO... SELECT... FROM..." statement for vehicles.

    Parameters:
        source_table: str
            The name of the table to SELECT FROM.

        dest_table: str, optional
            The name of the table to INSERT INTO, by default vehicles.

        on_conflict_update: tuple (condition: str, actions: str, list, dict), optional
            See on_conflict_statement().

        version: str, Version, optional
            The MDS version to target. By default, Version.mds_lower().

    Raise:
        UnsupportedVersionError
            When an unsupported MDS version is specified.

    Return:
        str
    """
    version = Version(kwargs.get("version", Version.mds_lower()))
    version.raise_if_unsupported()

    on_conflict_update = kwargs.get("on_conflict_update")

    inserts = list(_COMMON_INSERTS)
    inserts.extend([
        "last_event_time",
        "last_event_type",
        "last_event_type_reason",
        "last_event_location",
        "current_location",
        "battery_pct",
        "last_updated",
        "ttl"
    ])

    selects = list(_COMMON_SELECTS)
    selects.extend([
        "to_timestamp(cast(last_event_time as double precision) / 1000.0) at time zone 'UTC'",
        "cast(last_event_type as event_types)",
        "cast(last_event_type_reason as event_type_reasons)",
        "cast(last_event_location as jsonb)",
        "cast(current_location as jsonb)",
        "battery_pct",
        "to_timestamp(cast(last_updated as double precision) / 1000.0) at time zone 'UTC'",
        "ttl"
    ])

    return insert_statement(dest_table, inserts, selects, source_table, on_conflict_update)
