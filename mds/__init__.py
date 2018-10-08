"""
Tools for working with Mobility Data Specification Provider data.
"""

from mds._version import __version__


STATUS_CHANGES = "status_changes"

TRIPS = "trips"

VEHICLE_TYPES = ["bicycle", "scooter"]

PROPULSION_TYPES = ["human", "electric", "electric_assist", "combustion"]

EVENT_TYPE_REASONS = dict(available=["service_start",
                                     "user_drop_off",
                                     "rebalance_drop_off",
                                     "maintenance_drop_off"],
                          reserved=["user_pick_up"],
                          unavailable=["maintenance",
                                       "low_battery"],
                          removed=["service_end",
                                   "rebalance_pick_up",
                                   "maintenance_pick_up"])

EVENT_TYPES = list(EVENT_TYPE_REASONS.keys())


def VERSION():
    return __version__

