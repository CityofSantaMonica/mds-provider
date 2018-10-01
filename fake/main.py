import argparse
from fake.data import random_string, CustomJsonEncoder
from datetime import datetime, timedelta
from fake import geometry, provider
import json
import math
import mds
import os
import random
import time


if __name__ == "__main__":
    T0 = time.time()

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--boundary",
        type=str,
        help="Path to a data file with geographic bounds for the generated data.\
            Overrides the $MDS_BOUNDARY environment variable."
    )
    parser.add_argument(
        "--close",
        type=int,
        help="The hour of the day (24-hr format) that provider stops operations. Overrides --start and --end."
    )
    parser.add_argument(
        "--date_format",
        type=str,
        help="Format for datetime input (to this CLI) and output (to stdout and files). Options:\
            - 'unix' for Unix timestamps (default)\
            - 'iso8601' for ISO 8601 format\
            - '<python format string>' for custom formats,\
            see https://docs.python.org/3/library/datetime.html#strftime-strptime-behavior"
    )
    parser.add_argument(
        "--devices",
        type=int,
        help="The number of devices to model in the generated data"
    )
    parser.add_argument(
        "--end",
        type=str,
        help="The latest event in the generated data, in --date_format format"
    )
    parser.add_argument(
        "--inactivity",
        type=float,
        help="Describes the portion of the fleet that remains inactive\
            e.g. --inactivity=0.05 means 5 percent of the fleet remains inactive"
    )
    parser.add_argument(
        "--open",
        type=int,
        help="The hour of the day (24-hr format) that provider begins operations.\
            Overrides --start and --end."
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Path to a directory to write the resulting data file(s)"
    )
    parser.add_argument(
        "--propulsion_types",
        type=str,
        help="A list of propulsion_types to use for the generated data\
            e.g. '{}'".format("', '".join(mds.PROPULSION_TYPES))
    )
    parser.add_argument(
        "--provider",
        type=str,
        help="The name of the fake mobility as a service provider"
    )
    parser.add_argument(
        "--start",
        type=str,
        help="The earliest event in the generated data, in --date_format format"
    )
    parser.add_argument(
        "--speed_mph",
        type=float,
        help="The average speed of devices in miles per hour. Cannot be used with --speed_ms"
    )
    parser.add_argument(
        "--speed_ms",
        type=float,
        help="The average speed of devices in meters per second. Always takes precedence."
    )
    parser.add_argument(
        "--vehicle_types",
        type=str,
        help="A list of vehicle_types to use for the generated data:\
            e.g. '{}'".format("', '".join(mds.VEHICLE_TYPES))
    )

    args = parser.parse_args()
    print("Parsed args: {}".format(args))

    try:
        boundary_file = args.boundary or os.environ["MDS_BOUNDARY"]
    except:
        print("A boundary file is required")
        exit(1)

    # collect the parameters for data generation
    provider_name = args.provider or "Provider {}".format(random_string(3))
    N = args.devices or random.randint(100, 500)

    date_format = "unix" if args.date_format is None else args.date_format
    encoder = CustomJsonEncoder(date_format=date_format)

    date_start = datetime.today()
    date_end = date_start

    if date_format == "unix":
        date_start = datetime.fromtimestamp(args.start) if args.start else date_start
        date_end = datetime.fromtimestamp(args.end) if args.end else date_end
    elif date_format == "iso8601":
        date_start = datetime.fromisoformat(args.start) if args.start else date_start
        date_end = datetime.fromisoformat(args.end) if args.end else date_end
    else:
        date_start = datetime.strptime(args.start, date_format) if args.start else date_start
        date_end = datetime.strptime(args.end, date_format) if args.end else date_end

    hour_open = 7 if args.open is None else args.open
    hour_closed = 19 if args.close is None else args.close
    inactivity = random.uniform(0, 0.05) if args.inactivity is None else args.inactivity

    # convert speed to meters/second
    ONE_MPH_METERSSEC = 0.44704
    if args.speed_ms is not None:
        speed = args.speed_ms
    elif args.speed_mph is not None:
        speed = args.speed_mph * ONE_MPH_METERSSEC
    else:
        speed = random.uniform(8 * ONE_MPH_METERSSEC, 15 * ONE_MPH_METERSSEC)

    # setup a data directory
    outputdir = "data" if args.output is None else args.output
    os.makedirs(outputdir, exist_ok=True)

    print("Parsing boundary file: {}".format(boundary_file))
    t1 = time.time()
    boundary = geometry.parse_boundary(boundary_file, downloads=outputdir)
    print("Valid boundary: {} ({} s)".format(boundary.is_valid, time.time() - t1))

    gen = provider.DataGenerator(
            boundary=boundary,
            speed=speed,
            vehicle_types=args.vehicle_types,
            propulsion_types=args.propulsion_types)

    print("Generating {} devices for '{}'".format(N, provider_name))
    t1 = time.time()
    devices = gen.devices(N, provider_name)
    print("Generating devices complete ({} s)".format(time.time() - t1))

    status_changes, trips = [], []

    print("Generating data from {} to {}".format(encoder.encode(date_start), encoder.encode(date_end)))
    t1 = time.time()
    date = date_start
    while(date <= date_end):
        formatted_date = encoder.encode(date)
        print("Starting day: {} (open hours {} to {})".format(formatted_date, hour_open, hour_closed))
        t2 = time.time()
        day_status_changes, day_trips = \
            gen.service_day(devices, date, hour_open, hour_closed, inactivity)
        status_changes.extend(day_status_changes)
        trips.extend(day_trips)
        date = date + timedelta(days=1)
        print("Finished day: {} ({} s)".format(formatted_date, time.time() - t2))
    print("Finished generating data ({} s)".format(time.time() - t1))

    if len(status_changes) > 0 or len(trips) > 0:
        print("Generating data files")
        t1 = time.time()

        trips_file = os.path.join(outputdir, "trips.json")
        print("Writing to:", trips_file)
        t2 = time.time()
        with open(trips_file, "w") as f:
            payload = gen.make_payload(trips=trips)
            f.write(encoder.encode(payload))
        print("Finished ({} s)".format(time.time() - t2))

        sc_file = os.path.join(outputdir, "status_changes.json")
        print("Writing to:", sc_file)
        t2 = time.time()
        with open(sc_file, "w") as f:
            payload = gen.make_payload(status_changes=status_changes)
            f.write(encoder.encode(payload))
        print("Finished ({} s)".format(time.time() - t2))

        print("Generating data files complete ({} s)".format(time.time() - t1))

    print("Data generation complete ({} s)".format(time.time() - T0))

