from data import random_date_from, random_string
from datetime import datetime, timedelta
from geometry import point_within, point_nearby
import random
import uuid

VERSION = "0.1.0"
vehicle_types = ["bicycle", "scooter"]
propulsion_types = ["human", "electric", "electric_assist", "combustion"]
event_type_reasons = dict(available=  ["service_start",
                                       "user_drop_off",
                                       "rebalance_drop_off",
                                       "maintenance_drop_off"],
                          reserved=   ["user_pick_up"],
                          unavailable=["maintenance",
                                       "low_battery"],
                          removed=    ["service_end",
                                       "rebalance_pick_up",
                                       "maintenance_pick_up"])
class DataGenerator:
    """
    Generates fake MDS Provider data.
    """
    def __init__(self, boundary):
        """
        Initialize a new DataGenerator using the provided geographic :boundary:.
        """
        self.boundary = boundary

    def devices(self, N, provider):
        """
        Create a list of length :N: representing devices operated by :provider:.
        """
        devices = []
        provider_id = uuid.uuid4()
        for _ in range(N):
            devices.append(dict(provider_id=provider_id, 
                                provider_name=provider,
                                device_id=uuid.uuid4(),
                                vehicle_id=random_string(6),
                                vehicle_type=random.choice(vehicle_types),
                                propulsion_type=[random.choice(propulsion_types)]))
        return devices

    def status_changes(self, devices, datestart, dateend, 
                       houropen, hourclosed, inactivity):
        """
        Create service change events for the given :devices:, starting at 
        :houropen: on :datestart: until :hourclosed: on :dateend:. With :inactivity:
        percent of the devices remaining inactive.
        """
        status_changes = []

        service_start = self.start_service(devices, datestart, houropen)
        status_changes.extend(service_start)

        # these devices won't get any more events (left on the street)
        inactive_devices = random.sample(devices, int(len(devices)*inactivity))
        active_devices = [d for d in devices if d not in inactive_devices]

        if datestart.date != dateend.date:
            service_start = self.start_service(active_devices, dateend, houropen)
            status_changes.extend(service_start)

        service_end = self.end_service(active_devices, dateend, hourclosed)
        status_changes.extend(service_end)

        if datestart.date != dateend.date:
            service_end = self.end_service(active_devices, datestart, hourclosed)
            status_changes.extend(service_end)

        return dict(version=VERSION,
                    data=dict(status_changes=status_changes))

    def start_service(self, devices, date, hour):
        """
        Create service_change `available:service_start` events for each of :devices:. 
        """
        service_changes = []
        start = datetime(date.year, date.month, date.day, hour)
        # device placement starts before operation open time
        # -7200 seconds == previous 2 hours from start
        offset = timedelta(seconds=-7200)
        for device in devices:
            # somewhere in the previous :offset:
            event_time = random_date_from(start, min_td=offset)
            # the service_change details
            service_start = dict(event_type="available",
                                 event_type_reason="service_start",
                                 event_time=event_time,
                                 event_location=point_within(self.boundary),
                                 associated_trips=[])
            # reset the battery for electric devices
            if any("electric" in pt for pt in device["propulsion_type"]):
                device["battery_pct"] = 100.0
            # combine with device details and append
            service_changes.append({ **device, **service_start })
        return service_changes

    def end_service(self, devices, date, hour):
        """
        Create service_change `removed:service_end` events for each of :devices:. 
        """
        service_changes = []
        end = datetime(date.year, date.month, date.day, hour)
        # device pickup likely doesn't happen right at close time
        # +7200 seconds == next 2 hours after close
        offset = timedelta(seconds=7200)
        for device in devices:
            # somewhere in the next :offset:
            event_time = random_date_from(end, max_td=offset)
            # the service_change details
            service_end = dict(event_type="removed",
                               event_type_reason="service_end",
                               event_time=event_time,
                               event_location=point_within(self.boundary))
            # combine with device details and append
            service_changes.append({ **device, **service_end })
        return service_changes
