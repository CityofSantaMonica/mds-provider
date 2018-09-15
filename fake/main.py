import argparse
from data import random_string, JsonEncoder
from datetime import datetime, timedelta
import geometry
import json
import math
import mds
import os
import random
import time

T0 = time.time()

parser = argparse.ArgumentParser()

parser.add_argument(
    "--boundary",
    type=str,
    help="Path to a data file with geographic bounds for the generated data. \
          Overrides the $MDS_BOUNDARY environment variable."
)
parser.add_argument(
    "--provider",
    type=str,
    help="The name of the fake mobility as a service provider"
)
parser.add_argument(
    "--devices",
    type=int,
    help="The number of devices to model in the generated data"
)
parser.add_argument(
    "--start",
    type=str,
    help="YYYY-MM-DD of the earliest event in the generated data"
)
parser.add_argument(
    "--end",
    type=str,
    help="YYYY-MM-DD of the latest event in the generated data"
)
parser.add_argument(
    "--open",
    type=int,
    help="The hour of the day (24-hr format) that provider begins operations"
)
parser.add_argument(
    "--close",
    type=int,
    help="The hour of the day (24-hr format) that provider stops operations"
)
parser.add_argument(
    "--inactivity",
    type=float,
    help="The percent of the fleet that remains inactive; e.g. \
    --inactivity=0.05 means 5% of the fleet remains inactive"
)
parser.add_argument(
    "--output",
    type=str,
    help="Path to a directory to write the resulting data file(s)"
)
args = parser.parse_args()
print("Parsed args: {}".format(args))

try:
    boundary_file = args.boundary or os.environ["MDS_BOUNDARY"]
except:
    print("A boundary file is required")
    exit(1)

# collect the parameters for data generation
provider = args.provider or "Provider {}".format(random_string(3))
N = args.devices or int(random.uniform(500, 5000))
datestart = datetime.fromisoformat(args.start) if args.start else datetime.now()
dateend = datetime.fromisoformat(args.end) if args.end else (datestart + timedelta(days=1))
houropen = 7 if args.open is None else args.open
hourclosed = 19 if args.close is None else args.close
inactivity = random.uniform(0, 0.05) if args.inactivity is None else args.inactivity

# setup a data directory
outputdir = "data" if args.output is None else args.output
os.makedirs(outputdir, exist_ok=True)

print("Parsing boundary file: {}".format(boundary_file))
t1 = time.time()
boundary = geometry.parse_boundary(boundary_file, downloads=outputdir)
print("Valid boundary: {} ({} s)".format(boundary.is_valid, time.time() - t1))

gen = mds.DataGenerator(boundary)

print("Generating {} devices for '{}'".format(N, provider))
t1 = time.time()
devices = gen.devices(N, provider)
print("Generating devices complete ({} s)".format(time.time() - t1))

print("Generating status changes")
t1 = time.time()
status_changes = gen.status_changes(devices, datestart, dateend,
                                    houropen, hourclosed, inactivity)
print("Generating status changes complete ({} s)".format(time.time() - t1))

sc_file = os.path.join(outputdir, "status_changes.json")
print("Writing to:", sc_file)
with open(sc_file, "w") as f:
    json.dump(status_changes, f, cls=JsonEncoder)

print("Data generation complete ({} s)".format(time.time() - T0))