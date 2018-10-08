"""
SQL scripts for MDS Provider database CRUD.
"""

import mds


def insert_status_changes_from(source_table):
    """
    Generate an INSERT INTO statement from :source_table: to the Status Changes table, that
    ignores records that conflict based on existing uniqueness constraints.
    """
    return f"""
    INSERT INTO {mds.STATUS_CHANGES}
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
        provider_id::UUID,
        provider_name,
        device_id::UUID,
        vehicle_id,
        vehicle_type::vehicle_types,
        propulsion_type::propulsion_types[],
        event_type::event_types,
        event_type_reason::event_type_reasons,
        to_timestamp(event_time),
        event_location::JSON,
        battery_pct,
        associated_trips::UUID[]
    FROM {source_table}
    ON CONFLICT DO NOTHING;
    """

def insert_trips_from(source_table):
    """
    Generate an INSERT INTO statement from :source_table: to the Trips table, that
    ignores records that conflict based on existing uniqueness constraints.
    """
    return f"""
    INSERT INTO {mds.TRIPS}
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
        provider_id::UUID,
        provider_name,
        device_id::UUID,
        vehicle_id,
        vehicle_type::vehicle_types,
        propulsion_type::propulsion_types[],
        trip_id::UUID,
        trip_duration,
        trip_distance,
        route::JSON,
        accuracy,
        to_timestamp(start_time),
        to_timestamp(end_time),
        parking_verification_url,
        standard_cost,
        actual_cost
    FROM {source_table}
    ON CONFLICT DO NOTHING;
    """

