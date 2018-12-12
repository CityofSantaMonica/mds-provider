"""
SQL scripts for MDS Provider database CRUD.
"""

import mds


def insert_status_changes_from(source_table, dest_table=mds.STATUS_CHANGES, on_conflict_update=False):
    """
    Generate an INSERT INTO statement from :source_table: to the Status Changes table, that
    ignores records that conflict based on existing uniqueness constraints.
    """
    conflict_update = """ON CONFLICT DO UPDATE SET
    event_type = EXCLUDED.event_type::event_types,
    event_type_reason = EXCLUDED.event_type_reason::event_type_reasons,
    event_location = EXCLUDED.event_location::json,
    battery_pct = EXCLUDED.battery_pct,
    associated_trips = EXCLUDED.associated_trips::uuid[]
    """

    conflict_nothing = "ON CONFLICT DO NOTHING"

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
        provider_id::uuid,
        provider_name,
        device_id::uuid,
        vehicle_id,
        vehicle_type::vehicle_types,
        propulsion_type::propulsion_types[],
        event_type::event_types,
        event_type_reason::event_type_reasons,
        to_timestamp(event_time) AT TIME ZONE 'UTC',
        event_location::json,
        battery_pct,
        associated_trips::uuid[]
    FROM "{source_table}"
    { conflict_update if on_conflict_update else conflict_nothing }
    ;
    """

def insert_trips_from(source_table, dest_table=mds.TRIPS, on_conflict_update=False):
    """
    Generate an INSERT INTO statement from :source_table: to the Trips table, that
    ignores records that conflict based on existing uniqueness constraints.
    """
    conflict_update = """ON CONFLICT DO UPDATE SET
        trip_duration = EXCLUDED.trip_duration,
        trip_distance = EXCLUDED.trip_distance,
        route = EXCLUDED.route::json,
        accuracy = EXCLUDED.accuracy,
        start_time = to_timestamp(EXCLUDED.start_time) AT TIME ZONE 'UTC',
        end_time = to_timestamp(EXCLUDED.end_time) AT TIME ZONE 'UTC',
        parking_verification_url = EXCLUDED.parking_verification_url,
        standard_cost = EXCLUDED.standard_cost,
        actual_cost = EXCLUDED.actual_cost
    """

    conflict_nothing = "ON CONFLICT DO NOTHING"

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
        provider_id::uuid,
        provider_name,
        device_id::uuid,
        vehicle_id,
        vehicle_type::vehicle_types,
        propulsion_type::propulsion_types[],
        trip_id::uuid,
        trip_duration,
        trip_distance,
        route::json,
        accuracy,
        to_timestamp(start_time) AT TIME ZONE 'UTC',
        to_timestamp(end_time) AT TIME ZONE 'UTC',
        parking_verification_url,
        standard_cost,
        actual_cost
    FROM "{source_table}"
    { conflict_update if on_conflict_update else conflict_nothing }
    ;
    """

