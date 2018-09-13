import argparse
import geometry
import os

parser = argparse.ArgumentParser()

parser.add_argument(
    "--boundary",
    type=str,
    help="Path to a data file with geographic bounds for the generated data. \
          Overrides the $MDS_BOUNDARY environment variable."
)

args = parser.parse_args()
try:
    boundary_file = args.boundary or os.environ["MDS_BOUNDARY"]
except:
    print("A boundary file is required")
    exit(1)

print("running 'fake' with boundary_file:")
print(boundary_file)

boundary = geometry.make_boundary(boundary_file)
print("valid boundary: ", boundary.is_valid)

point = geometry.random_point_within(boundary)
print("(A) random interior point: ", point)

distance = 20
nearby_point = geometry.nearby_point(point, distance)
print("(B) random point {} meters away from (A): ".format(distance), nearby_point)