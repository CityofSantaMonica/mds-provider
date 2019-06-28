"""
Generating fake MDS Provider data.
"""

import datetime
import math
import random
import uuid

import scipy.stats

import mds.geometry
from ..fake import geometry, util
from ..schemas import Schema
from ..versions import Version


BATTERY = "battery_pct"
EVENT_LOC = "event_location"
EVENT_TIME = "event_time"
PUBLICATION_TIME = "publication_time"
PROPULSION = "propulsion_type"


class ProviderDataGenerator():
    """
    Generates fake MDS Provider data.
    """

    TD_HOUR = datetime.timedelta(seconds=3600)

    def __init__(self, boundary, **kwargs):
        """
        Initialize a new DataGenerator using the provided context.

        Parameters:
            boundary: str
                The path to a geoJSON file with boundary geometry within which to generate data.

            speed: int, optional
                The average speed of devices in meters/second.

            vehicle_types: str, list, optional
                Comma-separated string or list of vehicle_types to use for generation.

            propulsion_types: str, list, optional.
                Comma-separated string or list of propulsion_types to use for generation.

            version: str, Version, optional
                The MDS version to target. By default, use Version.mds_lower().
        """
        self.boundary = boundary
        self.version = Version(kwargs.pop("version", Version.mds_lower()))
        self.trips_schema = Schema.trips(self.version)
        self.speed = kwargs.get("speed")

        self.vehicle_types = kwargs.get("vehicle_types", self.trips_schema.vehicle_types)
        if isinstance(self.vehicle_types, str):
            self.vehicle_types = self.vehicle_types.split(",")

        self.propulsion_types = kwargs.get("propulsion_types", self.trips_schema.propulsion_types)
        if isinstance(self.propulsion_types, str):
            self.propulsion_types = self.propulsion_types.split(",")

    def __repr__(self):
        return f"<mds.fake.ProviderDataGenerator ('{self.version}')>"

    def devices(self, N, provider_name, provider_id=None):
        """
        Create a list of devices operated by a provider.

        Parameters:
            N: int
                The number of devices to generate.

            provider_name: str
                The name of the fictional provider.

            provider_id: str, UUID, optional
                The identifier of the fictional provider.

        Returns:
            list
                A list of dict each representing a device for the provider.
        """
        devices = []
        provider_id = provider_id or uuid.uuid4()

        for _ in range(N):
            device = dict(provider_id=provider_id,
                          provider_name=provider_name,
                          device_id=uuid.uuid4(),
                          vehicle_id=util.random_string(6),
                          vehicle_type=random.choice(self.vehicle_types),
                          propulsion_type=[random.choice(self.propulsion_types)])

            # ensure electric devices are charged
            if self.has_battery(device):
                self.recharge_battery(device)

            devices.append(device)

        return devices

    def service_day(self, devices, date, hour_open, hour_closed, inactivity):
        """
        Create status_change events and trips for a day of service.

        Parameters:
            devices: list
                A list of devices to put into services. See devices().

            date: date,
                The date on which to start service.

            hour_open: int
                The hour of the day that service begins.

            hour_closed: int
                The hour of the day that service ends.

            inactivity: float
                The percent of devices to mark as inactive for the day
                (i.e. start_service and end_service events at the same location with no trips).

        Returns:
            tuple (status_changes: list, trips: list)
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
        Create status_change events and trips for an hour of service.

        Parameters:
            times: list
                A list of datetime marking the earliest time for a new
                event for that device e.g. for recharge dropoff, trips have to start after
                (indexable by devices).

            locations: list
                The current location of each device (indexable by devices).

            inactivity: float
                A measure of how inactive the fleet is during this hour

        Returns:
            tuple (active_devices: list,
                   active_device_event_times: list,
                   active_device_event_locations: list,
                   removed_devices: list,
                   status_changes: list,
                   trips: list)
        """
        active, removed, changes, trips = [], [], [], []

        # chance of taking or not taking a trip
        weights = [1 - inactivity, inactivity]
        for device_idx in range(0, len(devices)):
            # assume this device will be active this hour
            device = devices[device_idx]
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
                trips.append(trip)
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
        Create status_change available:service_start events.

        Parameters:
            devices: list
                The list of devices to bring into service.

            start_time: datetime
                The approximate time of service start.

        Return:
            list
                The list of service_start events.
        """
        status_changes = []

        # device placement starts before operation open time
        # -7200 seconds == previous 2 hours from start
        offset = datetime.timedelta(seconds=-7200)
        for device in devices:
            # somewhere in the previous :offset:
            event_time = util.random_date_from(start_time, min_td=offset)
            point = geometry.point_within(self.boundary)
            feature = mds.geometry.to_feature(point, properties=dict(timestamp=event_time))

            # the status_change details
            service_start = \
                self.status_change_event(device,
                                         event_type="available",
                                         event_type_reason="service_start",
                                         event_time=event_time,
                                         event_location=feature)

            # reset the battery for electric devices
            if self.has_battery(device):
                self.recharge_battery(device)

            # combine with device details and append
            status_changes.append({**device, **service_start})

        return status_changes

    def end_service(self, devices, end_time, locations=None):
        """
        Create status_change removed:service_end events.

        Parameters:
            devices: list
                The list of devices to bring into service.

            end_time: datetime
                The approximate time of service end.

            locations: list, optional
                The corresponding end location for each device. By default, generate a random location.

        Returns:
            list
                The list of status_change events.
        """
        status_changes = []

        # device pickup likely doesn't happen right at close time
        # +7200 seconds == next 2 hours after close
        offset = datetime.timedelta(seconds=7200)
        for device in devices:
            # somewhere in the next :offset:
            event_time = util.random_date_from(end_time, max_td=offset)

            # use the device's index for the locations if provided
            # otherwise generate a random event_location
            if locations is None:
                point = geometry.point_within(self.boundary)
            else:
                point = mds.geometry.extract_point(locations[devices.index(device)])

            # the status_change details
            feature = mds.geometry.to_feature(point, properties=dict(timestamp=event_time))
            service_end = \
                self.status_change_event(device,
                                         event_type="removed",
                                         event_type_reason="service_end",
                                         event_time=event_time,
                                         event_location=feature)

            # combine with device details and append
            status_changes.append({**device, **service_end})

        return status_changes

    def device_trip(self, device, event_time=None, event_location=None,
                    end_location=None, reference_time=None, min_td=datetime.timedelta(seconds=0),
                    max_td=datetime.timedelta(seconds=0), speed=None):
        """
        Create a trip and associated status_changes for a device.

        Parameters:
            device: dict
                The device that will take the trip.

            event_time: datetime, optional
                The time the trip should start.

            event_location: GeoJSON Feature, optional
                The location the trip should start.

            end_location: GeoJSON Feature, optional
                The location the trip should end.

            reference_time: datetime, optional
                The 0-point around which to calculate a random start time.

            min_td: timedelta, optional
                The minimum time in the past from reference_time.

            max_td: timedelta, optional
                The maximum time in the future of reference_time.

            speed: int, optional
                The average speed of the device in meters/second.

        Returns:
            tuple (status_changes: list, trip: dict)
        """
        if (event_time is None) and (reference_time is not None):
            event_time = util.random_date_from(reference_time, min_td=min_td, max_td=max_td)

        if event_location is None:
            point = geometry.point_within(self.boundary)
            event_location = mds.geometry.to_feature(point, properties=dict(timestamp=event_time))

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

        # drain the battery according to the speed and distance traveled
        if self.has_battery(device):
            amount = speed / 100
            rate = trip_distance / (math.sqrt(trip_distance) * 200)
            self.drain_battery(device, amount=amount, rate=rate)

        # calculate out the ending time and location
        end_time = event_time + datetime.timedelta(seconds=trip_duration)
        if end_location is None:
            start_point = mds.geometry.extract_point(event_location)
            end_point = geometry.point_nearby(start_point, trip_distance, boundary=self.boundary)
            end_location = mds.geometry.to_feature(end_point, properties=dict(timestamp=end_time))

        # generate the route object
        route = self.trip_route(event_location, end_location)

        # and finally the trip object
        trip = dict(
            accuracy=int(accuracy),
            trip_id=uuid.uuid4(),
            trip_duration=int(trip_duration),
            trip_distance=int(trip_distance),
            route=route,
            start_time=event_time,
            end_time=end_time
        )

        if self.version >= Version("0.3.0"):
            trip[PUBLICATION_TIME] = end_time

        # add a parking_verification_url?
        if random.choice([True, False]):
            trip.update(parking_verification_url=util.random_file_url(device["provider_name"]))

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

        # cleanup leftover fields not part of a trip
        if BATTERY in trip:
            del trip[BATTERY]
        if EVENT_TIME in trip:
            del trip[EVENT_TIME]

        # return a list of the status_changes and the trip
        return [{**device, **sc} for sc in status_changes], trip

    def start_trip(self, device, event_time, event_location):
        """
        Create a reserved:user_pick_up status_change event.

        Parameters:
            device: dict
                The device that will take the trip.

            event_time: datetime
                The time the trip should start.

            event_location: GeoJSON Feature
                The location the trip should start.

        Returns:
            dict
                A reserved:user_pick_up status_change event.
        """
        return self.status_change_event(
            device,
            event_type="reserved",
            event_type_reason="user_pick_up",
            event_time=event_time,
            event_location=event_location
        )

    def trip_route(self, start_location, end_location):
        """
        Create GeoJSON FeatureCollection for the trip's route.

        Parameters:
            start_location: GeoJSON Feature
                The location the trip should start.

            end_location: GeoJSON Feature
                The location the trip should end.

        Returns:
            dict
                A GeoJSON FeatureCollection of the start and end locations.
        """
        features = [start_location, end_location]
        return dict(type="FeatureCollection", features=features)

    def end_trip(self, device, event_time, event_location):
        """
        Create an available:user_drop_off status_change event.

        Parameters:
            device: dict
                The device that took the trip.

            event_time: datetime
                The time the trip should end.

            event_location: GeoJSON Feature
                The location the trip should end.

        Returns:
            dict
                An available:user_drop_off status_change event.
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
        Create a unavailable:low_battery status_change event.

        Parameters:
            device: dict
                The device that has the low battery.

            event_time: datetime
                The time when the event occured.

            event_location: GeoJSON Feature
                The where location the event occured.

        Returns:
            dict
                An unavailable:low_battery status_change event.
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
        Create a available:maintenance_drop_off status_change event for each device.

        Parameters:
            devices: list
                The list of devices to recharge.

            event_times: datetime, list
                datetime: a reference to produce a random event time within the given hour.
                len(list) == len(devices): use the corresponding event_time for each device.

            event_locations: GeoJSON Feature, list, optional
                None: generate a random dropoff location for each device.
                Feature: use this as the dropoff location.
                len(list) == len(devices): use the corresponding event_location for each device.

        Returns:
            list
                A list of available:maintenance_drop_off status_change events.
        """
        status_changes = []

        for device in devices:
            if isinstance(event_times, datetime.datetime):
                # how many seconds until the next hour?
                diff = (60 - event_times.minute - 1)*60 + (60 - event_times.second)
                # random datetime between event_times and then
                event_time = util.random_date_from(event_times, max_td=datetime.timedelta(seconds=diff))
            elif len(event_times) == len(devices):
                # corresponding datetime
                event_time = event_times[devices.index(device)]

            if event_locations is None:
                # random point
                point = geometry.point_within(self.boundary)
                event_location = mds.geometry.to_feature(point, properties=dict(timestamp=event_time))
            elif len(event_locations) == len(devices):
                # corresponding location
                event_location = event_locations[devices.index(device)]
            else:
                # given location
                event_location = event_locations

            # create the event for this device
            status_changes.append(self.device_recharged(device, event_time, event_location))

        return status_changes

    def device_recharged(self, device, event_time, event_location):
        """
        Create an available:maintenance_drop_off status_change event.

        Parameters:
            device: dict
                The device that was recharged.

            event_time: datetime
                The time when the event occured.

            event_location: GeoJSON Feature
                The where location the event occured.

        Returns:
            dict
                An available:maintenance_drop_off status_change event.
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
        Create a status_change event from the provided data.

        Parameters:
            device: dict
                The device that generated the event.

            event_type:
                The type of status_change event.
                See https://github.com/CityOfLosAngeles/mobility-data-specification/tree/master/provider#event-types

            event_type_reason:
                The reason for this type of status_change event.
                See https://github.com/CityOfLosAngeles/mobility-data-specification/tree/master/provider#event-types

            event_time: datetime
                The time when the event occured.

            event_location: GeoJSON Feature
                The location where the event occured.

            Additional keyword parameters are passed into the event as attributes.

        Returns:
            dict
                A dict representation of the status_change data.
        """
        status_change = dict(event_type=event_type,
                             event_type_reason=event_type_reason,
                             event_time=event_time,
                             event_location=event_location)

        if self.version >= Version("0.3.0"):
            status_change[PUBLICATION_TIME] = event_time

        return {**device, **status_change, **kwargs}

    def has_battery(self, device):
        """
        Determine if device has a battery.
        """
        return BATTERY in device or \
               any("electric" in pt for pt in device[PROPULSION]) if PROPULSION in device else False

    def recharge_battery(self, device):
        """
        Recharge device's battery to full.
        """
        device[BATTERY] = 1.0

    def drain_battery(self, device, amount=0.0, rate=0.0):
        """
        Drain device's battery by an absolute amount and/or by a constant rate:

            new_battery = (current_battery - amount) * (1 - rate)
        """
        if self.has_battery(device):
            device[BATTERY] = (device[BATTERY] - amount) * (1 - rate)

    def make_payload(self, status_changes=None, trips=None):
        """
        Craft the full MDS Provider payload for either status_changes or trips.
        """
        payload = dict(version=str(self.version))

        if status_changes is not None:
            payload["data"] = dict(status_changes=status_changes)

        if trips is not None:
            payload["data"] = dict(trips=trips)

        return payload
