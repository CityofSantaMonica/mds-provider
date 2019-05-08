"""
Work with MDS Provider database backends.
"""

import json
import os
import pandas as pd
from pathlib import Path
import sqlalchemy
import string

from ..fake import random_string
from ..files import ProviderDataFiles
from ..providers import Provider
from ..schemas import STATUS_CHANGES, TRIPS
from ..versions import UnexpectedVersionError, UnsupportedVersionError, Version

from .sql import insert_status_changes_from, insert_trips_from


def data_engine(uri=None, **kwargs):
    """
    Create an engine for connections to a database backend.

    Parameters:
        uri: str, optional
            A DBAPI-compatible connection URI.

            e.g. for a PostgreSQL backend: postgresql://user:password@host:port/db

            Required if any of (user, password, host, db) are not provided.

        backend: str, optional
            The type of the database backend. By default, postgresql.

        user: str, optional
            The user account for the database backend.

        password: str, optional
            The password for the user account.

        host: str, optional
            The host name of the database backend.

        port: int, optional
            The database backend connection port. By default, 5432 (postgres).

        db: str, optional
            The name of the database to connect to.

    Returns:
        sqlalchemy.engine.Engine
    """
    if uri is None and all(k in kwargs for k in ["user", "password", "host", "db"]):
        backend = kwargs.pop("backend", "postgresql")
        user, password, host, port, db = kwargs["user"], kwargs["password"], kwargs["host"], kwargs.get("port", 5432), kwargs["db"]
        uri = f"{backend}://{user}:{password}@{host}:{port}/{db}"
    elif uri is None:
        raise KeyError("Provide either uri or ([backend], user, password, host, [port], db).")

    return sqlalchemy.create_engine(uri)


class ProviderDatabase():
    """
    Work with MDS Provider data in a database backend.
    """

    def __init__(self, uri=None, **kwargs):
        """
        Initialize a new ProviderDataLoader using a number of connection methods.

        Parameters:
            uri: str, optional
                A DBAPI-compatible connection URI.

                e.g. for a PostgreSQL backend: postgresql://user:password@host:port/db

                Required if engine or any of (user, password, host, db) are not provided.

            backend: str, optional
                The type of the database backend. By default, postgresql.

            user: str, optional
                The user account for the database backend.

            password: str, optional
                The password for the user account.

            host: str, optional
                The host name of the database backend.

            port: int, optional
                The database backend connection port. By default, 5432 (postgres).

            db: str, optional
                The name of the database to connect to.

            stage_first: bool, int, optional
                True (default) to stage data in a temp table before upserting to the final table.
                False to load directly into the target table.

                Given an int greater than 0, determines the degrees of randomness when creating the
                temp table, e.g.

                    stage_first=3

                stages to a random temp table with 26*26*26 possible naming choices.

            version: str, Version, optional
                The MDS version to target. By default, Version.mds_lower().

        Raises:
            UnsupportedVersionError
                When an unsupported MDS version is specified.
        """
        self.version = Version(kwargs.pop("version", Version.mds_lower()))
        if self.version.unsupported:
            raise UnsupportedVersionError(self.version)

        self.stage_first = kwargs.pop("stage_first", True)
        self.engine = kwargs.pop("engine", data_engine(uri=uri, **kwargs))

    def load_from_df(self, df, record_type, table, **kwargs):
        """
        Inserts MDS data from a DataFrame.

        Parameters:
            df: DataFrame
                Data of type record_type to insert.

            record_type: str
                The type of MDS data, e.g. status_changes or trips

            table: str
                The name of the database table to insert this data into.

            before_load: callable(df=DataFrame, version=Version): DataFrame, optional
                Callback executed on the incoming DataFrame and Version.
                Should return the final DataFrame for loading.

            stage_first: bool, int, optional
                True (default) to stage data in a temp table before upserting to the final table.
                False to load directly into the target table.

                Given an int greater than 0, determines the degrees of randomness when creating the
                temp table, e.g.

                    stage_first=3

                stages to a random temp table with 26*26*26 possible naming choices.

            on_conflict_update: tuple (condition: str, actions: list), optional
                Generate an "ON CONFLICT condition DO UPDATE SET actions" statement.
                Only applies when stage_first evaluates True.

            version: str, Version, optional
                The MDS version to target.

        Raises:
            UnsupportedVersionError
                When an unsupported MDS version is specified.

        Returns:
            ProviderDataLoader
                self
        """
        version = Version(kwargs.get("version", self.version))
        if version.unsupported:
            raise UnsupportedVersionError(version)

        before_load = kwargs.get("before_load", None)
        stage_first = kwargs.get("stage_first", self.stage_first)
        on_conflict_update = kwargs.get("on_conflict_update", None)

        # run any pre-processors to transform the df
        if before_load is not None:
            new_df = before_load(df, version)
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
                if record_type == STATUS_CHANGES:
                    query = insert_status_changes_from(temp, table, on_conflict_update=on_conflict_update, version=version)
                elif record_type == TRIPS:
                    query = insert_trips_from(temp, table, on_conflict_update=on_conflict_update, version=version)
                if query is not None:
                    conn.execute(query)
                    # delete temp table (not a true TEMPORARY table)
                    conn.execute(f"DROP TABLE {temp}")
        return self

    def load_from_file(self, src, record_type, table, **kwargs):
        """
        Load MDS data from a file source.

        Parameters:
            src: str
                An mds.json.files_to_df() compatible JSON file path.

            record_type: str
                The type of MDS data, e.g. status_changes or trips

            table: str
                The name of the table to load data to.

            version: str, Version, optional
                The MDS version to target.

            Additional keyword arguments are passed-through to load_from_df().

        Raises:
            UnexpectedVersionError
                When data is parsed with a version different from what was expected.

            UnsupportedVersionError
                When an unsupported MDS version is specified.

        Returns:
            ProviderDataLoader
                self
        """
        version = Version(kwargs.get("version", self.version))
        if version.unsupported:
            raise UnsupportedVersionError(version)

        # read the data file
        _version, df = ProviderDataFiles(src).load_dataframe(record_type)

        if _version != version:
            raise UnexpectedVersionError(_version, version)

        return self.load_from_df(df, record_type, table, **kwargs)

    def load_from_records(self, records, record_type, table, **kwargs):
        """
        Load MDS data from a list of records.

        Parameters:
            records: list
                A list of dicts of type record_type.

            record_type: str
                The type of MDS data, e.g. status_changes or trips

            table: str
                The name of the table to load data to.

            Additional keyword arguments are passed-through to load_from_df().

        Raises:
            TypeError
                When records is not a list of dicts.

        Returns:
            ProviderDataLoader
                self
        """
        if isinstance(records, list) and len(records) > 0 and all([isinstance(d, dict) for d in records]):
            df = pd.DataFrame.from_records(records)
            self.load_from_df(df, record_type, table, **kwargs)
            return self

        raise TypeError(f"Unknown type for records: {type(records)}")

    def load_from_source(self, source, record_type, table, **kwargs):
        """
        Load MDS data from a variety of file path or object sources.

        Parameters:
            source: dict, list, str, Path
                The data source to load, which could be any of:
                    - an MDS payload dict, e.g.
                        {
                            "version": "x.y.z",
                            "data": {
                                "record_type": [{
                                    "device_id": "1",
                                    ...
                                },
                                {
                                    "device_id": "2",
                                    ...
                                }]
                            }
                        }
                    - a list of MDS payloads
                    - a list of MDS data records, e.g. payload["data"][record_type]
                    - a [list of] MDS payload JSON file paths

            record_type: str
                The type of MDS data, e.g. status_changes or trips

            table: str
                The name of the table to load data to.

            version: str, Version, optional
                The MDS version to target.

            Additional keyword arguments are passed-through to load_from_df().

        Raises:
            TypeError
                When the type of source is not recognized.

            UnexpectedVersionError
                When data is parsed with a version different from what was expected.

            UnsupportedVersionError
                When an unsupported MDS version is specified.

        Returns:
            ProviderDataLoader
                self
        """
        version = Version(kwargs.get("version", self.version))
        if version.unsupported:
            raise UnsupportedVersionError(version)

        def _valid_path(p):
            """
            Check for a valid path reference
            """
            return (isinstance(p, str) and os.path.exists(p)) or (isinstance(p, Path) and p.exists())

        # source is a single data page
        if isinstance(source, dict) and "data" in source and record_type in source["data"]:
            _version, records = Version(source["version"]), source["data"][record_type]
            if _version != version:
                raise UnexpectedVersionError(_version, version)
            self.load_from_records(records, record_type, table, **kwargs)

        # source is a list of data pages
        elif isinstance(source, list) and all([isinstance(s, dict) and "data" in s for s in source]):
            for page in source:
                self.load_from_source(page, record_type, table, **kwargs)

        # source is a list of file paths, load only the valid paths
        elif isinstance(source, list) and any([_valid_path(p) for p in source]):
            for path in [p for p in source if _valid_path(p)]:
                self.load_from_source(path, record_type, table, **kwargs)

        # source is a single (valid) file path
        elif _valid_path(source):
            self.load_from_file(source, record_type, table, **kwargs)

        # source is something else we can't handle
        else:
            raise TypeError(f"Unrecognized type for source: {type(source)}")

        return self

    def load_status_changes(self, source, **kwargs):
        """
        Load MDS status_changes data.

        Parameters:
            source: dict, list, str, Path
                See load_from_sources for supported source types.

            table: str, optional
                The name of the table to load data to, by default status_changes.

            before_load: callable(df=DataFrame, version=Version): DataFrame, optional
                Callback executed on the incoming DataFrame and Version.
                Should return the final DataFrame for loading.

            drop_duplicates: list, optional
                List of column names used to drop duplicate records before load.

            version: str, Version, optional
                The MDS version to target.

            Additional keyword arguments are passed-through to load_from_df().

        Returns:
            ProviderDataLoader
                self
        """
        version = Version(kwargs.get("version", self.version))
        if version.unsupported:
            raise UnsupportedVersionError(version)

        table = kwargs.pop("table", STATUS_CHANGES)
        before_load = kwargs.pop("before_load", lambda df,v: df)
        drop_duplicates = kwargs.pop("drop_duplicates", None)

        def _before_load(df,v):
            """
            Helper converts JSON cols and ensures optional cols exist
            """
            if drop_duplicates: df.drop_duplicates(subset=drop_duplicates, keep="last", inplace=True)
            self._json_cols_tostring(df, ["event_location"])
            missing_cols = ["battery_pct"]
            missing_cols.append("associated_trips" if version < Version("0.3.0") else "associated_trip")
            df = self._add_missing_cols(df, missing_cols)
            return before_load(df,v)

        return self.load_from_source(source, STATUS_CHANGES, table, before_load=_before_load, **kwargs)

    def load_trips(self, source, **kwargs):
        """
        Load MDS trips data.

        Parameters:
            source: dict, list, str, Path
                See load_from_sources for supported source types.

            table: str, optional
                The name of the table to load data to, by default trips.

            before_load: callable(df=DataFrame, version=Version): DataFrame, optional
                Callback executed on the incoming DataFrame and Version.
                Should return the final DataFrame for loading.

            drop_duplicates: list, optional
                List of column names used to drop duplicate records before load.
                By default, ["provider_id", "trip_id"]

            Additional keyword arguments are passed-through to load_from_df().

        Returns:
            ProviderDataLoader
                self
        """
        table = kwargs.pop("table", TRIPS)
        before_load = kwargs.pop("before_load", lambda df,v: df)
        drop_duplicates = kwargs.pop("drop_duplicates", ["provider_id", "trip_id"])

        def _before_load(df,v):
            """
            Helper converts JSON cols and ensures optional cols exist
            """
            if drop_duplicates: df.drop_duplicates(subset=drop_duplicates, keep="last", inplace=True)
            self._json_cols_tostring(df, ["route"])
            df = self._add_missing_cols(df, ["parking_verification_url", "standard_cost", "actual_cost"])
            return before_load(df,v)

        return self.load_from_source(source, TRIPS, table, before_load=_before_load, **kwargs)

    @staticmethod
    def _json_cols_tostring(df, cols):
        """
        For each cols in the df, convert to a JSON string.
        """
        for col in [c for c in cols if c in df]:
            df[col] = df[col].apply(json.dumps)
        return df

    @staticmethod
    def _add_missing_cols(df, cols):
        """
        For each cols not in the df, add as an empty col.
        """
        new_cols = set(df.columns.tolist() + cols)
        return df.reindex(columns=new_cols)
