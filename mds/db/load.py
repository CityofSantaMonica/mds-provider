"""
Load MDS Provider data into a database.
"""

import json
import mds
from mds.db import sql
from mds.fake.data import random_string
from mds.json import read_data_file
import os
import pandas as pd
from pathlib import Path
import sqlalchemy
import string


def data_engine(uri=None, **kwargs):
    """
    Create a SQLAlchemy engine using the provided DBAPI-compatible connection uri.
    E.g. `postgresql://user:password@host:port/db` for a PostgreSQL backend.

    Also supports connecting via keyword arguments [:backend:], :user:, :password:, :host:, :port:, :db:.

    If no :backend: is given, the default is `postgresql`.

    :returns: An `sqlalchemy.engine.Engine` instance.
    """
    if uri is None and all(k in kwargs for k in ["user", "password", "host", "port", "db"]):
        backend = kwargs["backend"] if "backend" in kwargs else "postgresql"
        user, password, host, port, db = kwargs["user"], kwargs["password"], kwargs["host"], kwargs["port"], kwargs["db"]
        uri = f"{backend}://{user}:{password}@{host}:{port}/{db}"
    elif uri is None:
        raise KeyError("Provide either `uri` or `user`, `password`, `host`, `port`, and `db`.")

    return sqlalchemy.create_engine(uri)


class ProviderDataLoader():
    """
    A class for loading MDS Provider data.
    """

    def __init__(self, uri=None, **kwargs):
        """
        Initialize a new `ProviderDataLoader` using a number of connection methods.

        Optional positional arguments:

        :uri: A DBAPI-compatible connection URI
        E.g. `postgresql://user:password@host:port/db` for a PostgreSQL backend.

        Optional keyword arguments:

        :engine: An `sqlalchemy.engine.Engine` instance.

        :backend: The name of a the SQL backend, e.g. `postgres`.

        :user: The user account to connect with.

        :password: The password for the user account.

        :host: The SQL host.

        :port: The SQL host port.

        :db: The name of the database to connect to.

        You must provide either:
          - :uri:
          - :engine:
          - all of :user:, :password:, :host:, :port:, and :db:

        :stage_first: True (default) to stage the load into a temp table before upserting to the final destination. False
        to load directly into the target table. Given an int greater than 0, determines the degrees of randomness when
        creating the temp table, e.g.

        `stage_first=3`

        stages to a random temp table with 26*26*26 possible naming choices.
        """
        if "engine" in kwargs:
            self.engine = kwargs["engine"]
        else:
            self.engine = data_engine(uri=uri, **kwargs)

        self.stage_first = kwargs.get("stage_first", True)

    def _json_cols_tostring(self, df, cols):
        """
        For each :cols: in the :df:, convert to a JSON string.
        """
        for col in [c for c in cols if c in df]:
            df[col] = df[col].apply(json.dumps)

    def _add_missing_cols(self, df, cols):
        """
        For each :cols: not in the :df:, add it as an empty col.
        """
        new_cols = set(df.columns.tolist() + cols)
        return df.reindex(columns=new_cols)

    def load_from_df(self, df, record_type, table, **kwargs):
        """
        Inserts MDS data from a DataFrame.

        Required positional arguments:

        :df: The `pandas.DataFrame` of data of type :record_type: to insert.

        :record_type: The type of MDS data - either `status_changes` or `trips`.

        :table: The name of the database table to insert this data into.

        Optional keyword arguments:

        :before_load: Transform callback executed on the incoming :df:; should return the final `pandas.DataFrame` to be
        loaded to the database.

        :stage_first: True (default) to stage the load into a temp table before upserting to the final destination. False
        to load directly into the target table. Given an int greater than 0, determines the degrees of randomness when
        creating the temp table, e.g.

        `stage_first=3`

        stages to a random temp table with 26*26*26 possible naming choices.

        :on_conflict_update: tuple (condition, actions) for an ON CONFLICT :condition: DO UPDATE SET :actions: statement.
        Only applies when :stage_first: evaluates True.
        """
        before_load = kwargs.get("before_load", None)
        stage_first = kwargs.get("stage_first", self.stage_first)
        on_conflict_update = kwargs.get("on_conflict_update", None)

        # run any pre-processors to transform the df
        if before_load is not None:
            new_df = before_load(df)
            df = new_df if new_df is not None else df

        if not stage_first:
            # append the data to an existing table
            df.to_sql(table, self.engine, if_exists="append", index=False)
        else:
            # insert this DataFrame into a fresh temp table
            factor = stage_first if isinstance(stage_first, int) else 1
            temp = f"{table}_tmp_{random_string(factor, chars=string.ascii_lowercase)}"
            df.to_sql(temp, self.engine, if_exists="replace", index=False)

            # now insert from the temp table to the actual table
            with self.engine.begin() as conn:
                if record_type == mds.STATUS_CHANGES:
                    query = sql.insert_status_changes_from(temp, table, on_conflict_update)
                elif record_type == mds.TRIPS:
                    query = sql.insert_trips_from(temp, table, on_conflict_update)
                if query is not None:
                    conn.execute(query)

                # Delete temp table since not using a real TEMPORARY
                conn.execute(f"DROP TABLE {temp}")

    def load_from_file(self, src, record_type, table, **kwargs):
        """
        Load MDS data from a file source.

        Required positional arguments:

        :src: Path to a JSON file of status_change or trip data. See `mds.json.read_data_file()` for
        more details.

        :record_type: One of `status_changes` or `trips`.

        :table: The name of the table to load data to.

        Additional keyword arguments are passed-through to `load_from_df`.
        """
        # read the data file
        _, df = read_data_file(src, record_type)
        self.load_from_df(df, record_type, table, **kwargs)

    def load_from_records(self, records, record_type, table, **kwargs):
        """
        Load a list of MDS records.

        Required positional arguments:

        :records: A list of status_change or trip objects.

        :record_type: One of `status_changes` or `trips`.

        :table: The name of the table to load data to.

        Additional keyword arguments are passed-through to `load_from_df`.
        """
        if isinstance(records, list):
            if len(records) > 0:
                df = pd.DataFrame.from_records(records)
                self.load_from_df(df, record_type, table, **kwargs)
            else:
                print("No records to load")

    def load_from_source(self, source, record_type, table, **kwargs):
        """
        Load MDS data from a variety of file path or object sources.

        Required positional arguments:

        :source: The data source to load, which could be any of:

            - a list of json file paths

            - a data page (the contents of a json file), e.g.
                {
                    "version": "x.y.z",
                    "data": {
                        "record_type": [{
                            "trip_id": "1",
                            ...
                        },
                        {
                            "trip_id": "2",
                            ...
                        }]
                    }
                }

            - a list of data pages

            - a dict of { Provider : [data page] }

        :record_type: One of `status_changes` or `trips`.

        :table: The name of the table to load data to.

        Additional keyword arguments are passed-through to `load_from_df`.
        """
        def __valid_path(p):
            """
            Check for a valid path reference
            """
            return (isinstance(p, str) and os.path.exists(p)) or (isinstance(p, Path) and p.exists())

        # source is a single data page
        if isinstance(source, dict) and "data" in source and record_type in source["data"]:
            records = source["data"][record_type]
            self.load_from_records(records, record_type, table, **kwargs)

        # source is a list of data pages
        elif isinstance(source, list) and all([isinstance(s, dict) and "data" in s for s in source]):
            for page in source:
                self.load_from_source(page, record_type, table, **kwargs)

        # source is a dict of Provider => list of data pages
        elif isinstance(source, dict) and all(isinstance(k, mds.providers.Provider) for k in source.keys()):
            for _, pages in source.items():
                self.load_from_source(pages, record_type, table, **kwargs)

        # source is a list of file paths
        elif isinstance(source, list) and any([__valid_path(p) for p in source]):
            # load only the valid paths
            for path in [p for p in source if __valid_path(p)]:
                self.load_from_source(path, record_type, table, **kwargs)

        # source is a single (valid) file path
        elif __valid_path(source):
            self.load_from_file(source, record_type, table, **kwargs)

        else:
            print(f"Couldn't recognize source with type '{type(source)}'. Skipping.")

    def load_status_changes(self, source, **kwargs):
        """
        Load MDS status_changes data.

        Required positional arguments:

        :source: The data source to load, which could be any of:

            - a list of json file paths

            - a data page (the contents of a json file), e.g.
                {
                    "version": "x.y.z",
                    "data": {
                        "status_changes": [{
                            "provider_id": "...",
                            ...
                        },
                        {
                            "provider_id": "...",
                            ...
                        }]
                    }
                }

            - a list of data pages

            - a dict of { Provider : [data page] }

        Optional keyword arguments:

        :table: The name of the table to load data to, by default `mds.STATUS_CHANGES`.

        Additional keyword arguments are passed-through to `load_from_df`.
        """
        table = kwargs.pop("table", mds.STATUS_CHANGES)
        before_load = kwargs.pop("before_load", None)

        def __before_load(df):
            """
            Helper converts JSON cols and ensures optional cols exist
            """
            df.drop_duplicates(subset=["provider_id", "device_id", "event_time"], keep="last", inplace=True)
            self._json_cols_tostring(df, ["event_location"])
            df = self._add_missing_cols(df, ["battery_pct", "associated_trips"])
            df[["associated_trips"]] = df[["associated_trips"]].astype("object")
            df["associated_trips"] = df["associated_trips"].apply(lambda d: d if isinstance(d, list) else [])
            return before_load(df) if before_load else df

        self.load_from_source(source, mds.STATUS_CHANGES, table, before_load=__before_load, **kwargs)

    def load_trips(self, source, **kwargs):
        """
        Load MDS trips data.

        Required positional arguments:

        :source: The data source to load, which could be any of:

            - a list of json file paths

            - a data page (the contents of a json file), e.g.
                {
                    "version": "x.y.z",
                    "data": {
                        "trips": [{
                            "provider_id": "...",
                            ...
                        },
                        {
                            "provider_id": "...",
                            ...
                        }]
                    }
                }

            - a list of data pages

            - a dict of { Provider : [data page] }

        Optional keyword arguments:

        :table: The name of the table to load data to, by default `mds.TRIPS`.

        Additional keyword arguments are passed-through to `load_from_df`.
        """
        table = kwargs.pop("table", mds.TRIPS)
        before_load = kwargs.pop("before_load", None)

        def __before_load(df):
            """
            Helper converts JSON cols and ensures optional cols exist
            """
            df.drop_duplicates(subset=["provider_id", "trip_id"], keep="last", inplace=True)
            self._json_cols_tostring(df, ["route"])
            df = self._add_missing_cols(df, ["parking_verification_url", "standard_cost", "actual_cost"])
            return before_load(df) if before_load else df

        self.load_from_source(source, mds.TRIPS, table, before_load=__before_load, **kwargs)
