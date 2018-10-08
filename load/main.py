import argparse
from mds.db import db
from mds.validate import validate_status_changes, validate_trips, validate_files
import os
import sys
import time


def expand_all_files(sources):
    """
    Return a list of all the files from a potentially mixed list of files and directories.
    Expands into the directories and includes their files in the output, as well as any input files.
    """
    # separate
    files = [f for f in sources if os.path.isfile(f)]
    dirs = [d for d in sources if os.path.isdir(d)]

    # expand and extend
    expanded = [f for ls in [os.listdir(d) for d in dirs] for f in ls]
    files.extend(expanded)

    return files


if __name__ == "__main__":
    try:
        host = os.environ["POSTGRES_HOSTNAME"]
    except:
        print("The POSTGRES_HOSTNAME environment variable is not set. Exiting.")
        exit(1)

    try:
        port = os.environ["POSTGRES_HOST_PORT"]
    except:
        port = 5432
        print("No POSTGRES_HOST_PORT environment variable set, defaulting to:", port)

    try:
        db_name = os.environ["MDS_DB"]
    except:
        print("The MDS_DB environment variable is not set. Exiting.")
        exit(1)

    try:
        user, password = os.environ["MDS_USER"], os.environ["MDS_PASSWORD"]
    except:
        print("The MDS_USER or MDS_PASSWORD environment variables are not set. Exiting.")
        exit(1)

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--status_changes",
        nargs="+",
        help="One or more paths to (directories containing) status changes JSON file(s)"
    )
    parser.add_argument(
        "--trips",
        nargs="+",
        help="One or more paths to (directories containing) trips JSON file(s)"
    )
    parser.add_argument(
        "--no_validate",
        action="store_true",
        help="Do not perform JSON Schema validation against the input file(s)"
    )

    args = parser.parse_args()
    print(args)

    if args.status_changes is None and args.trips is None:
        parser.print_help(sys.stderr)
        exit(1)

    engine = db.data_engine(user, password, db_name, host, port)

    if args.status_changes is not None:
        files = expand_all_files(args.status_changes)

        print("Status Changes load context:", files)

        if args.no_validate:
            print("Skipping schema validation")
            valid = files
        else:
            valid, invalid = validate_files(files, validate_status_changes)

            if len(invalid) > 0:
                print(f"Found invalid file(s) ({len(invalid)} of {len(valid) + len(invalid)}):")
                print(invalid)
                print("Skipping load for invalid file(s)")

        db.load_status_changes(valid, engine)

    if args.trips is not None:
        files = expand_all_files(args.trips)

        print("Trips load context:", files)

        if args.no_validate:
            print("Skipping schema validation")
            valid = files
        else:
            valid, invalid = validate_files(files, validate_trips)

            if len(invalid) > 0:
                print(f"Found invalid file(s) ({len(invalid)} of {len(valid) + len(invalid)}):")
                print(invalid)
                print("Skipping load for invalid file(s)")

        db.load_trips(valid, engine)

