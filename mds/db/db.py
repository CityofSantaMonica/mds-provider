"""
Work with MDS Provider database backends.
"""

import json

import sqlalchemy

from ..db import loaders
from ..providers import Provider
from ..schemas import STATUS_CHANGES, TRIPS
from ..versions import UnsupportedVersionError, Version


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

    Return:
        sqlalchemy.engine.Engine
    """
    if uri is None and all(k in kwargs for k in ["user", "password", "host", "db"]):
        backend = kwargs.pop("backend", "postgresql")
        user, password, host, port, db = kwargs["user"], kwargs["password"], kwargs["host"], kwargs.get("port", 5432), kwargs["db"]
        uri = f"{backend}://{user}:{password}@{host}:{port}/{db}"
    elif uri is None:
        raise KeyError("Provide either uri or ([backend], user, password, host, [port], db).")

    return sqlalchemy.create_engine(uri)


class Database():
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

        Raise:
            UnsupportedVersionError
                When an unsupported MDS version is specified.
        """
        self.version = Version(kwargs.pop("version", Version.mds_lower()))
        if self.version.unsupported:
            raise UnsupportedVersionError(self.version)

        self.stage_first = kwargs.pop("stage_first", True)
        self.engine = kwargs.pop("engine", data_engine(uri=uri, **kwargs))

    def __repr__(self):
        return f"<mds.db.Database ('{self.version}')>"

    def load(self, source, record_type, table, **kwargs):
        """
        Load MDS data from a variety of file path or object sources.

        Parameters:
            source: dict, list, str, Path, pandas.DataFrame
                The data source to load, which could be any of:
                * an MDS payload dict:
                    {
                        "version": "x.y.z",
                        "data": {
                            "record_type": [
                                //records here
                            ]
                        }
                    }
                * a list of MDS payload dicts
                * one or more MDS data records, e.g. payload["data"][record_type]
                * one or more file paths to MDS payload JSON files
                * a pandas.DataFrame containing MDS data records

            record_type: str
                The type of MDS data, e.g. status_changes or trips

            record_type: str
                The type of MDS data ("status_changes" or "trips").

            table: str
                The name of the database table to insert this data into.

            before_load: callable(df=DataFrame, version=Version): DataFrame, optional
                Callback executed on an incoming DataFrame and Version.
                Should return the final DataFrame for loading.

            on_conflict_update: tuple (condition: str, actions: list), optional
                Generate an "ON CONFLICT condition DO UPDATE SET actions" statement.
                Only applies when stage_first evaluates True.

            stage_first: bool, int, optional
                True (default) to stage data in a temp table before upserting to the final table.
                False to load directly into the target table.

                Given an int greater than 0, determines the degrees of randomness when creating the
                temp table, e.g.

                    stage_first=3

                stages to a random temp table with 26*26*26 possible naming choices.

            version: str, Version, optional
                The MDS version to target. By default, Version.mds_lower().

        Raise:
            TypeError
                When a loader for the type of source could not be found.

            UnsupportedVersionError
                When an unsupported MDS version is specified.

        Return:
            Database
                self
        """
        version = Version(kwargs.get("version", self.version))
        if version.unsupported:
            raise UnsupportedVersionError(version)

        if "stage_first" not in kwargs:
            kwargs["stage_first"] = self.stage_first

        for loader in loaders.data_loaders():
            if loader.can_load(source):
                loader().load(source, record_type=record_type, table=table, engine=self.engine, **kwargs)
                return self

        raise TypeError(f"Unrecognized type for source: {type(source)}")

    def load_status_changes(self, source, **kwargs):
        """
        Load MDS status_changes data.

        Parameters:
            source: dict, list, str, Path, pandas.DataFrame
                See load() for supported source types.

            table: str, optional
                The name of the table to load data to. By default "status_changes".

            before_load: callable(df=DataFrame, version=Version): DataFrame, optional
                Callback executed on the incoming DataFrame and Version.
                Should return the final DataFrame for loading.

            drop_duplicates: list, optional
                List of column names used to drop duplicate records before load.

            version: str, Version, optional
                The MDS version to target.

            Additional keyword arguments are passed-through to load().

        Return:
            Database
                self
        """
        table = kwargs.pop("table", STATUS_CHANGES)
        before_load = kwargs.pop("before_load", lambda df,v: df)
        drop_duplicates = kwargs.pop("drop_duplicates", None)

        version = kwargs.get("version", self.version)
        if version.unsupported:
            raise UnsupportedVersionError(version)

        def _before_load(df,v):
            """
            Helper converts JSON cols and ensures optional cols exist
            """
            if drop_duplicates:
                df.drop_duplicates(subset=drop_duplicates, keep="last", inplace=True)

            self._json_cols_tostring(df, ["event_location"])

            missing_cols = ["battery_pct"]

            # version-depenedent association column
            association_col = "associated_trips" if version < Version("0.3.0") else "associated_trip"
            missing_cols.append(association_col)
            df = self._add_missing_cols(df, missing_cols)

            # coerce to object column
            df[[association_col]] = df[[association_col]].astype("object")

            if version < Version("0.3.0"):
                # empty list by default
                df[association_col] = df[association_col].apply(lambda d: d if isinstance(d, list) else [])

            return before_load(df,v)

        return self.load(source, STATUS_CHANGES, table, before_load=_before_load, **kwargs)

    def load_trips(self, source, **kwargs):
        """
        Load MDS trips data.

        Parameters:
            source: dict, list, str, Path, pandas.DataFrame
                See load() for supported source types.

            table: str, optional
                The name of the table to load data to, by default trips.

            before_load: callable(df=DataFrame, version=Version): DataFrame, optional
                Callback executed on the incoming DataFrame and Version.
                Should return the final DataFrame for loading.

            drop_duplicates: list, optional
                List of column names used to drop duplicate records before load.
                By default, ["provider_id", "trip_id"]

            Additional keyword arguments are passed-through to load().

        Return:
            Database
                self
        """
        table = kwargs.pop("table", TRIPS)
        before_load = kwargs.pop("before_load", lambda df,v: df)
        drop_duplicates = kwargs.pop("drop_duplicates", ["provider_id", "trip_id"])

        def _before_load(df,v):
            """
            Helper converts JSON cols and ensures optional cols exist
            """
            if drop_duplicates:
                df.drop_duplicates(subset=drop_duplicates, keep="last", inplace=True)

            self._json_cols_tostring(df, ["route"])
            df = self._add_missing_cols(df, ["parking_verification_url", "standard_cost", "actual_cost"])

            return before_load(df,v)

        return self.load(source, TRIPS, table, before_load=_before_load, **kwargs)

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
