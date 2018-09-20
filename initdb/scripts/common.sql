CREATE TYPE vehicle_types AS ENUM (
    'bicycle',
    'scooter'
);

CREATE TYPE propulsion_types AS ENUM (
    'human',
    'electric_assist',
    'electric',
    'combustion'
);

CREATE TYPE event_types AS ENUM (
    'available',
    'reserved',
    'unavailable',
    'removed'
);

CREATE TYPE event_type_reasons AS ENUM (
    'service_start',
    'maintenance_drop_off',
    'rebalance_drop_off',
    'user_drop_off',
    'user_pick_up',
    'maintenance',
    'low_battery',
    'service_end',
    'rebalance_pick_up',
    'maintenance_pick_up'
);