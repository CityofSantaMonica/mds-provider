CREATE TABLE IF NOT EXISTS trips (
    provider_id UUID NOT NULL,
    provider_name TEXT NOT NULL,
    device_id UUID NOT NULL,
    vehicle_id TEXT NOT NULL,
    vehicle_type vehicle_types NOT NULL,
    propulsion_type propulsion_types[] NOT NULL,
    trip_id UUID NOT NULL,
    trip_duration INT NOT NULL,
    trip_distance INT NOT NULL,
    route JSON NOT NULL,
    accuracy INT NOT NULL,
    start_time BIGINT NOT NULL,
    end_time BIGINT NOT NULL,
    parking_verification_url TEXT,
    standard_cost INT,
    actual_cost INT
);