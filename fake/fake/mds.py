from data import random_date_from, random_string, random_file_url
from datetime import datetime, timedelta
from geometry import point_within, point_nearby
import math
import random
import uuid
import scipy.stats

VERSION = "0.1.0"
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

BATTERY = "battery_pct"
EVENT_LOC = "event_location"
EVENT_TIME = "event_time"
PROPULSION = "propulsion_type"

class DataGenerator:
    """
    Generates fake MDS Provider data.
    """


    TD_HOUR = timedelta(seconds=3600)

    def __init__(self, **kwargs):
        """
        Initialize a new DataGenerator using the provided context. 

        Required keyword arguments:
            - :boundary: is the geographic boundary within which to generate data
              see geometry.parse_boundary(:boundary_file:)

        Optional keyword arguments:
            - :speed: the average speed of devices (in meters/second)
            - :vehicle_types: the vehicle_types to use for generation
            - :propulsion_types: the propulsion_types to use for generation
        """
        key = "boundary"
        if not key in kwargs:
            raise("A geographic boundary is required")
        self.boundary = kwargs[key]

        key = "speed"
        if key in kwargs and kwargs[key] is not None:
            self.speed = kwargs[key]

        key = "vehicle_types"
        if key in kwargs and kwargs[key] is not None:
            self.vehicle_types = kwargs[key].split(",")\
                if isinstance(kwargs[key], str) else kwargs[key]
        else:
            self.vehicle_types = VEHICLE_TYPES

        key = "propulsion_types"
        if key in kwargs and kwargs[key] is not None:
            self.propulsion_types = kwargs[key].split(",")\
                if isinstance(kwargs[key], str) else kwargs[key]
        else:
            self.propulsion_types = PROPULSION_TYPES

    def devices(self, N, provider):
        """
        Create a list of length :N: representing devices operated by :provider:.
        """
        devices = []
        provider_id = uuid.uuid4()
        for _ in range(N):
            device = dict(provider_id=provider_id,
                          provider_name=provider,
                          device_id=uuid.uuid4(),
                          vehicle_id=random_string(6),
                          vehicle_type=random.choice(self.vehicle_types),
                          propulsion_type=[random.choice(self.propulsion_types)])
            if self.has_battery(device):
                self.recharge_battery(device)
            devices.append(device)
        return devices

    def service_day(self, devices, date, hour_open, hour_closed, inactivity):
        """
        Create status change events and trips on :date: between the hours of 
        :houropen: and :hourclosed: for the given :devices:.

        :inactivity: the percent of devices to mark as inactive for the day
                     (i.e. start_service and end_service events at the same location with no trips).

        Returns a tuple:
            - status_changes = list of status_change objects for the day
            - trips = list of trip objects for the day
        """
        day_status_changes, day_trips = [], []
        start_time = date.replace(hour=hour_open)
        end_time = date.replace(hour=hour_closed)
        # partition the devices into inactive and active
        # inactive will only get start/end service events in the same location
        inactive_devices = random.sample(devices, int(len(devices)*inactivity))
        inactive_starts = self.start_service(inactive_devices, start_time)
        inactive_locations = [e[EVENT_LOC] for e in inactive_starts]
        inactive_ends = self.end_service(inactive_devices, end_time, inactive_locations)
        day_status_changes.extend(inactive_starts + inactive_ends)
        # all the rest of the devices that participate in the service day
        active_devices = [d for d in devices if d not in inactive_devices]
        start_events = self.start_service(active_devices, start_time)
        day_status_changes.extend(start_events)
        # the list of event_time from the prior event for each device
        # initialized to the beginning of the day
        times = [start_time for e in start_events]
        # the list of event_location from the prior event for each device
        locations = [e[EVENT_LOC] for e in start_events]
        # devices removed from service during a given hour
        removed_devices = []
        # model each hour of the day (including the last)
        for hour in range(hour_open, hour_closed + 1):
            # some devices may be recharged and put back into service this hour
            recharged = random.sample(removed_devices, random.randint(0, len(removed_devices)))
            if len(recharged) > 0:
                # re-activate these for the hour
                active_devices.extend(recharged)
                # generate the placement events
                events = self.devices_recharged(recharged, [r[EVENT_TIME] for r in recharged])
                day_status_changes.extend(events)
                times.extend([e[EVENT_TIME] for e in events])
                locations.extend([e[EVENT_LOC] for e in events])
                # update the list of removed devices
                removed_devices = [d for d in removed_devices if d not in recharged]
            # generate data for the hour
            active_devices, times, locations, removed, hour_changes, hour_trips = \
                self.service_hour(
                    active_devices,
                    date,
                    hour,
                    times,
                    locations,
                    inactivity
                )
            day_status_changes.extend(hour_changes)
            day_trips.extend(hour_trips)
            removed_devices.extend(removed)
        # end service for the remaining active devices
        day_status_changes.extend(self.end_service(active_devices, end_time, locations))
        return day_status_changes, day_trips

    def service_hour(self, devices, date, hour, times, locations, inactivity):
        """
        Create status change events and trips for the given :devices: on :date:
        during the hour of :hour:.

        :times: is a list of datetime corresponding to :devices:, marking the
                earliest time for a new event for that device
                (e.g. for recharge dropoff, trips have to start after)

        :locations: is a list of the current location of each device (indexable by :devices:).

        :inactivity: is a measure of how inactive the fleet is during this hour

        Returns a tuple:
            - the devices remaining active after this hour
            - active device event times for this hour
            - active device event locations for this hour
            - devices removed this hour
            - status changes for this hour
            - trips starting this hour
        """
        active, removed, changes, trips = [], [], [], []
        # chance of taking or not taking a trip
        weights = [1 - inactivity, inactivity]
        for device_idx in range(0, len(devices)):
            device = devices[device_idx]
            # assume this device will be active this hour
            active.append(device)
            location = locations[device_idx]
            current_time = times[device_idx]
            # check the device's charge level
            if self.has_battery(device) and device[BATTERY] < 0.2:
                # battery is too low -> deactivate
                lowbattery = self.device_lowbattery(device, current_time, location)
                # update the state for this event and device
                del active[active.index(device)]
                rmvd = dict(device)
                rmvd.update(event_time=lowbattery[EVENT_TIME])
                removed.append(rmvd)
                changes.append(lowbattery)
                continue
            # will this device take a trip?
            if random.choices([True, False], weights=weights, k=1)[0]:
                # yes, it will -- sometime this hour
                status, trip = self.device_trip(device,
                                                event_location=location,
                                                reference_time=current_time,
                                                max_td=self.TD_HOUR)
                changes.extend(status)
                trips.append({**device, **trip})
                # update the device's time and location from the trip's end event
                times[device_idx] = status[-1][EVENT_TIME]
                locations[device_idx] = status[-1][EVENT_LOC]
            elif self.has_battery(device):
                # no, it won't take a trip -- leak some power anyway
                self.drain_battery(device, rate=random.uniform(0, 0.05))
        # return all the data for this hour
        return (active,
                [times[devices.index(a)] for a in active],
                [locations[devices.index(a)] for a in active],
                removed,
                changes,
                trips)

    def start_service(self, devices, start_time):
        """
        Create service_change `available:service_start` events on or around :starttime:
        for each of the :devices:.
        """
        service_changes = []
        # device placement starts before operation open time
        # -7200 seconds == previous 2 hours from start
        offset = timedelta(seconds=-7200)
        for device in devices:
            # somewhere in the previous :offset:
            event_time = random_date_from(start_time, min_td=offset)
            # the service_change details
            service_start = \
                self.status_change_event(device,
                                         event_type="available",
                                         event_type_reason="service_start",
                                         event_time=event_time,
                                         event_location=point_within(self.boundary),
                                         associated_trips=[])
            # reset the battery for electric devices
            if self.has_battery(device):
                self.recharge_battery(device)
            # combine with device details and append
            service_changes.append({**device, **service_start})
        return service_changes

    def end_service(self, devices, end_time, locations=None):
        """
        Create service_change `removed:service_end` events for each of :devices:.

        If :locations: are provided, use the corresponding location for each device.
        """
        service_changes = []
        # device pickup likely doesn't happen right at close time
        # +7200 seconds == next 2 hours after close
        offset = timedelta(seconds=7200)
        for device in devices:
            # somewhere in the next :offset:
            event_time = random_date_from(end_time, max_td=offset)
            # use the device's index for the locations if provided
            # otherwise generate a random event_location
            if locations is None:
                event_location = point_within(self.boundary)
            else:
                event_location = locations[devices.index(device)]
            # the service_change details
            service_end = \
                self.status_change_event(device,
                                         event_type="removed",
                                         event_type_reason="service_end",
                                         event_time=event_time,
                                         event_location=event_location)
            # combine with device details and append
            service_changes.append({**device, **service_end})
        return service_changes

    def device_trip(self, device, event_time=None, event_location=None,
                    end_location=None, reference_time=None, min_td=timedelta(seconds=0),
                    max_td=timedelta(seconds=0), speed=None):
        """
        Create a trip and associated status changes for the given :device:.

        :event_time: is the time the trip should start

        :event_location: is the point the trip should start
        
        :end_location: is the point the trip should end
        
        :reference_time: is the 0-point around which to calculate a random start time
            - :min_td: the minimum time from :reference_time:
            - :max_td: the maximum time from :reference_time:

        :speed: is the average speed of the device in meters/second

        Returns a tuple:
            - the list of status changes
            - the trip
        """
        if (event_time is None) and (reference_time is not None):
            event_time = random_date_from(reference_time, min_td=min_td, max_td=max_td)
        if event_location is None:
            event_location = point_within(self.boundary)
        if speed is None:
            speed = self.speed
        # begin the trip
        status_changes = [self.start_trip(device, event_time, event_location)]
        # random trip duration (in seconds)
        # the gamma distribution is referenced in the literature,
        # see: https://static.tti.tamu.edu/tti.tamu.edu/documents/17-1.pdf
        # experimenting with the scale factors led to these parameterizations, * 60 to get seconds
        alpha, beta = 3, 4.5
        trip_duration = random.gammavariate(alpha, beta) * 60
        # account for traffic, turns, etc.
        trip_distance = trip_duration * speed * 0.8
        # Model the accuracy as a rayleigh distribution with median ~5m
        accuracy = scipy.stats.rayleigh.rvs(scale=5)
        if self.has_battery(device):
            # drain the battery according to the speed and distance traveled
            amount = speed / 100
            rate = trip_distance / (math.sqrt(trip_distance) * 200)
            self.drain_battery(device, amount=amount, rate=rate)
        # calculate out the ending time and location
        end_time = event_time + timedelta(seconds=trip_duration)
        if end_location is None:
            end_location = point_nearby(event_location, trip_distance)
        # generate the route object
        route = self.trip_route(device, event_time, event_location, end_time, end_location)
        # and finally the trip object
        trip = dict(
            accuracy=accuracy,
            trip_id=uuid.uuid4(),
            trip_duration=trip_duration,
            trip_distance=trip_distance,
            route=route,
            start_time=event_time,
            end_time=end_time
        )
        # add a parking_verification_url?
        if random.choice([True, False]):
            trip.update(parking_verification_url=random_file_url(device["provider_name"]))
        # add a standard_cost?
        if random.choice([True, False]):
            # $1.00 to start and $0.15 a minute thereafter
            trip.update(standard_cost=(100 + (math.floor(trip_duration/60) - 1) * 15))
        # add an actual cost?
        if random.choice([True, False]):
            # randomize an actual_cost
            # $0.75 - $1.50 to start, and $0.12 - $0.20 a minute thereafter...
            start, rate = random.randint(75, 150), random.randint(12, 20)
            trip.update(actual_cost=(start + (math.floor(trip_duration/60) - 1) * rate))
        # end the trip
        status_changes.append(self.end_trip(device, end_time, end_location))
        # merge the device info into the trip
        trip = {**device, **trip}
        # battery info is not part of the trip
        if self.has_battery(trip):
            del trip[BATTERY]

        return [{**device, **sc} for sc in status_changes], trip

    def start_trip(self, device, event_time, event_location):
        """
        Create a `reserved:user_pick_up` status change event.
        """
        return self.status_change_event(
            device,
            event_type="reserved",
            event_type_reason="user_pick_up",
            event_time=event_time,
            event_location=event_location
        )

    def trip_route(self, device, start_time, start_location, end_time, end_location):
        features = []
        for e in [(start_time, start_location), (end_time, end_location)]:
            features.append(
                dict(type="Feature",
                     properties=dict(timestamp=e[0]),
                     geometry=dict(type="Point",
                                   coordinates=[e[1].x, e[1].y]
                     )
                )
            )
        return dict(type="FeatureCollection", features=features)

    def end_trip(self, device, event_time, event_location):
        """
        Create a `available:user_drop_off` status change event.
        """
        return self.status_change_event(
            device,
            event_type="available",
            event_type_reason="user_drop_off",
            event_time=event_time,
            event_location=event_location
        )

    def device_lowbattery(self, device, event_time, event_location):
        """
        Create a `unavailable:low_battery` status change event.
        """
        return self.status_change_event(
            device,
            event_type="unavailable",
            event_type_reason="low_battery",
            event_time=event_time,
            event_location=event_location
        )

    def devices_recharged(self, devices, event_times, event_locations=None):
        """
        Create a `available:maintenance_drop_off` status change event for each of :devices:.

        :event_times: is an single or list of datetimes:
            - single datetime: use this as a reference to produce a random event time within the given hour
            - list with len == len(devices): use the corresponding event_time for each device

        :event_locations: is an (optional) single or list of locations:
            - None: generate a random dropoff location for each device
            - single location: use this as the dropoff location
            - list with len == len(devices): use the corresponding event_location for each device
        """
        service_changes = []

        for device in devices:
            if isinstance(event_times, datetime):
                # how many seconds until the next hour?
                diff = (60 - event_times.minute - 1)*60 + (60 - event_times.second)
                # random datetime between event_times and then
                event_time = random_date_from(event_times, max_td=timedelta(seconds=diff))
            elif len(event_times) == len(devices):
                # corresponding datetime
                event_time = event_times[devices.index(device)]

            if event_locations is None:
                # random point
                event_location = point_within(self.boundary)
            elif len(event_locations) == len(devices):
                # corresponding point
                event_location = event_locations[devices.index(device)]
            else:
                # given point
                event_location = event_locations
            # create the event for this device
            service_changes.append(self.device_recharged(device, event_time, event_location))

        return service_changes

    def device_recharged(self, device, event_time, event_location):
        """
        Create a `available:maintenance_drop_off` status change event.
        """
        self.recharge_battery(device)
        return self.status_change_event(
            device,
            event_type="available",
            event_type_reason="maintenance_drop_off",
            event_time=event_time,
            event_location=event_location
        )

    def status_change_event(self,
                            device,
                            event_type,
                            event_type_reason,
                            event_time,
                            event_location,
                            **kwargs):
        """
        Create a status change event from the provided data.
        """
        status_change = dict(event_type=event_type,
                             event_type_reason=event_type_reason,
                             event_time=event_time,
                             event_location=event_location)
        return {**device, **status_change, **kwargs}

    def has_battery(self, obj):
        """
        Determine if :obj: has a battery.
        """
        return BATTERY in obj or \
               any("electric" in pt for pt in obj[PROPULSION]) if PROPULSION in obj else False

    def recharge_battery(self, obj):
        """
        Recharge :obj:'s battery to full
        """
        obj[BATTERY] = 1.0

    def drain_battery(self, obj, amount=0.0, rate=0.0):
        """
        Drain :obj:'s battery by an absolute :amount: and/or by a constant :rate:.

        e.g. new_battery = current_battery - amount
             new_battery = current_battery * (1 - rate)
             new_battery = (current_battery - amount) * (1 - rate)
        """
        if self.has_battery(obj):
            obj[BATTERY] = (obj[BATTERY] - amount) * (1 - rate)
