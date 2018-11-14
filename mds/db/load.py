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
    else:
        raise KeyError()

    return sqlalchemy.create_engine(uri)


class ProviderDataLoader():
    """
    A class for loading MDS Provider data.
    """

    def __init__(self, uri=None, **kwargs):
        """
        Initialize a new `ProviderDataLoader` using a number of connection methods.

        The default positional argument :uri:, a DBAPI-compatible connection URI
        E.g. `postgresql://user:password@host:port/db` for a PostgreSQL backend.

        Provide an `sqlalchemy.engine.Engine` instance via the :engine: keyword argument.

        Or use the raw connection values :backend:, :user:, :password:, :host:, :port:, :db:.
        """
        if "engine" in kwargs:
            self.engine = kwargs["engine"]
        else:
            self.engine = data_engine(uri=uri, **kwargs)

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

    def load_from_df(self, df, record_type, table, before_load=None, stage_first=True):
        """
        Inserts data from a DataFrame matching the given MDS :record_type: schema to the :table:
        using the connection specified in :enging:.

        :before_load: is an optional transform to perform on the DataFrame before inserting its data.

        :stage_first: when True, implements a staged upsert via a temp table. The default is True.
        """
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
                    query = sql.insert_status_changes_from(temp, table)
                elif record_type == mds.TRIPS:
                    query = sql.insert_trips_from(temp, table)
                if query is not None:
                    conn.execute(query)

                # Delete the tmptable  since we did fake tmp tables
                # and not using Postgres TEMPORARY 
                conn.execute(f"DROP TABLE {temp}")


    def load_from_file(self, src, record_type, table, before_load=None, stage_first=True):
        """
        Load the data file of type :record_type: at :src: in the table :table: using the connection
        defined by :engine:.

        :before_load: is an optional callback to pre-process a DataFrame before loading
        it into :table:.
        """
        # read the data file
        _, df = read_data_file(src, record_type)
        self.load_from_df(df, record_type, table,
                          before_load=before_load, stage_first=stage_first)

    def load_from_records(self, records, record_type, table, before_load=None, stage_first=True):
        """
        Load the array of :records: of :record_type: into the table :table: using the connection defined by :engine:.

        :before_load: is an optional callback to pre-process a DataFrame before loading
        it into :table:.
        """
        if isinstance(records, list):
            if len(records) > 0:
                df = pd.DataFrame.from_records(records)
                self.load_from_df(
                    df, record_type, table, before_load=before_load, stage_first=stage_first)
            else:
                print("No records to load")

    def load_from_source(self, source, record_type, table, before_load=None, stage_first=True):
        """
        Load from a variety of file path or object sources into a :table: using the connection defined by conn.

        :source: could be:

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
        """
        def __valid_path(p):
            """
            Check for a valid path reference
            """
            return (isinstance(p, str) and os.path.exists(p)) or (isinstance(p, Path) and p.exists())

        # source is a single data page
        if isinstance(source, dict) and "data" in source and record_type in source["data"]:
            records = source["data"][record_type]
            self.load_from_records(
                records, record_type, table, before_load=before_load, stage_first=stage_first)

        # source is a list of data pages
        elif isinstance(source, list) and all([isinstance(s, dict) and "data" in s for s in source]):
            for page in source:
                self.load_from_source(
                    page, record_type, table, before_load=before_load, stage_first=stage_first)

        # source is a dict of Provider => list of data pages
        elif isinstance(source, dict) and all(isinstance(k, mds.providers.Provider) for k in source.keys()):
            for _, pages in source.items():
                self.load_from_source(
                    pages, record_type, table, before_load=before_load, stage_first=stage_first)

        # source is a list of file paths
        elif isinstance(source, list) and any([__valid_path(p) for p in source]):
            # load only the valid paths
            for path in [p for p in source if __valid_path(p)]:
                self.load_from_source(
                    path, record_type, table, before_load=before_load, stage_first=stage_first)

        # source is a single (valid) file path
        elif __valid_path(source):
            self.load_from_file(
                source, record_type, table, before_load=before_load, stage_first=stage_first)

        else:
            print(f"Couldn't recognize source with type '{type(source)}'. Skipping.")

    def load_status_changes(self, sources, table=mds.STATUS_CHANGES, before_load=None, stage_first=True):
        """
        Load status_changes data from :sources: using the connection defined by :engine:.

        By default, stages the load into a temp table before upserting to the final destination.
        """
        def __before_load(df):
            """
            Helper converts JSON cols and ensures optional cols exist
            """
            self._json_cols_tostring(df, ["event_location"])
            df = self._add_missing_cols(df, ["battery_pct", "associated_trips"])
            df[["associated_trips"]] = df[["associated_trips"]].astype("object")
            df["associated_trips"] = df["associated_trips"].apply(lambda d: d if isinstance(d, list) else [])
            return before_load(df) if before_load else df

        self.load_from_source(sources, mds.STATUS_CHANGES, table,
                              before_load=__before_load, stage_first=stage_first)

    def load_trips(self, sources, table=mds.TRIPS, before_load=None, stage_first=True):
        """
        Load trips data from :sources: using the connection defined by :engine:.

        By default, stages the load into a temp table before upserting to the final destination.
        """
        def __before_load(df):
            """
            Helper converts JSON cols and ensures optional cols exist
            """
            self._json_cols_tostring(df, ["route"])
            df = self._add_missing_cols(df, ["parking_verification_url", "standard_cost", "actual_cost"])
            return before_load(df) if before_load else df

        self.load_from_source(sources, mds.TRIPS, table,
                              before_load=__before_load, stage_first=stage_first)
