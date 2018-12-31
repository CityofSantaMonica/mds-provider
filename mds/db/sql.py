"""
SQL scripts for MDS Provider database CRUD.
"""

import mds


__conflict_nothing = "ON CONFLICT DO NOTHING"


def insert_status_changes_from(source_table, dest_table=mds.STATUS_CHANGES, on_conflict_update=False):
    """
    Generate an INSERT INTO statement from :source_table: to the Status Changes table.
    """
    conflict_update = """ON CONFLICT (provider_id, device_id, event_time) DO UPDATE SET
        event_type = cast(EXCLUDED.event_type as event_types),
        event_type_reason = cast(EXCLUDED.event_type_reason as event_type_reasons),
        event_location = cast(EXCLUDED.event_location as json),
        battery_pct = EXCLUDED.battery_pct,
        associated_trips = cast(EXCLUDED.associated_trips as uuid[])
    """

    return f"""
    INSERT INTO "{dest_table}"
    (
        provider_id,
        provider_name,
        device_id,
        vehicle_id,
        vehicle_type,
        propulsion_type,
        event_type,
        event_type_reason,
        event_time,
        event_location,
        battery_pct,
        associated_trips
    )
    SELECT
        cast(provider_id as uuid),
        provider_name,
        cast(device_id as uuid),
        vehicle_id,
        cast(vehicle_type as vehicle_types),
        cast(propulsion_type as propulsion_types[]),
        cast(event_type as event_types),
        cast(event_type_reason as event_type_reasons),
        to_timestamp(event_time) at time zone 'UTC',
        cast(event_location as json),
        battery_pct,
        cast(associated_trips as uuid[])
    FROM "{source_table}"
    { conflict_update if on_conflict_update else __conflict_nothing }
    ;
    """

def insert_trips_from(source_table, dest_table=mds.TRIPS, on_conflict_update=False):
    """
    Generate an INSERT INTO statement from :source_table: to the Trips table.
    """
    conflict_update = """ON CONFLICT (provider_id, trip_id) DO UPDATE SET
        trip_duration = EXCLUDED.trip_duration,
        trip_distance = EXCLUDED.trip_distance,
        route = cast(EXCLUDED.route as json),
        accuracy = EXCLUDED.accuracy,
        start_time = EXCLUDED.start_time,
        end_time = EXCLUDED.end_time,
        parking_verification_url = EXCLUDED.parking_verification_url,
        standard_cost = EXCLUDED.standard_cost,
        actual_cost = EXCLUDED.actual_cost
    """

    return f"""
    INSERT INTO "{dest_table}"
    (
        provider_id,
        provider_name,
        device_id,
        vehicle_id,
        vehicle_type,
        propulsion_type,
        trip_id,
        trip_duration,
        trip_distance,
        route,
        accuracy,
        start_time,
        end_time,
        parking_verification_url,
        standard_cost,
        actual_cost
    )
    SELECT
        cast(provider_id as uuid),
        provider_name,
        cast(device_id as uuid),
        vehicle_id,
        cast(vehicle_type as vehicle_types),
        cast(propulsion_type as propulsion_types[]),
        cast(trip_id as uuid),
        trip_duration,
        trip_distance,
        cast(route as json),
        accuracy,
        to_timestamp(start_time) at time zone 'UTC',
        to_timestamp(end_time) at time zone 'UTC',
        parking_verification_url,
        standard_cost,
        actual_cost
    FROM "{source_table}"
    { conflict_update if on_conflict_update else __conflict_nothing }
    ;
    """
