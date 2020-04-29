"""
Format-specific data loading for MDS Provider database backends.
"""

import string

import pandas as pd

from ..db import sql
from ..fake import util
from ..files import DataFile
from ..schemas import STATUS_CHANGES, TRIPS, EVENTS, Schema
from ..versions import UnexpectedVersionError, Version


class DataFrame():
    """
    A data loader for pandas.DataFrame instances.

    To implement a new data loader, create a subclass of DataFrameLoader and implement:

        load(self, source, **kwargs)
            Initialize a DataFrame from source.
            Call super().load(df, **kwargs).

        @classmethod
        can_load(cls, source): bool
            Return True if the data loader can load data from source.

    See FileLoader for an example implementation.
    """

    def load(self, source, **kwargs):
        """
        Inserts MDS data from a DataFrame.

        Parameters:
            source: DataFrame
                DataFrame of type record_type to insert.

            record_type: str
                The type of MDS data.

            table: str
                The name of the database table to insert this data into.

            engine: sqlalchemy.engine.Engine
                The engine used for connections to the database backend.

            before_load: callable(df=DataFrame, version=Version): DataFrame, optional
                Callback executed on the incoming DataFrame and Version.
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
            UnsupportedVersionError
                When an unsupported MDS version is specified.
        """
        record_type = kwargs.pop("record_type")
        table = kwargs.pop("table")
        engine = kwargs.pop("engine")

        version = Version(kwargs.get("version", Version.mds_lower()))
        version.raise_if_unsupported()

        before_load = kwargs.get("before_load")
        stage_first = kwargs.get("stage_first")
        on_conflict_update = kwargs.get("on_conflict_update")

        # run any pre-processors to transform the df
        if before_load is not None:
            transform = before_load(source, version)
            source = source if transform is None else transform

        if not stage_first:
            # append the data to an existing table
            source.to_sql(table, engine, if_exists="append", index=False)
            return

        # insert this DataFrame into a fresh temp table
        factor = stage_first if isinstance(stage_first, int) else 1
        temp = f"{table}_tmp_{util.random_string(factor, chars=string.ascii_lowercase)}"
        source.to_sql(temp, engine, if_exists="replace", index=False)

        # now insert from the temp table to the actual table
        with engine.begin() as conn:
            if record_type in [STATUS_CHANGES, EVENTS]:
                query = sql.insert_status_changes_from(temp,
                                                       table,
                                                       version=version,
                                                       on_conflict_update=on_conflict_update)
            elif record_type == TRIPS:
                query = sql.insert_trips_from(temp,
                                              table,
                                              version=version,
                                              on_conflict_update=on_conflict_update)
            if query is not None:
                # move data using query and delete temp table
                conn.execute(query)
                conn.execute(f"DROP TABLE {temp}")

    @classmethod
    def can_load(cls, source):
        """
        True if source is a pandas.DataFrame
        """
        return isinstance(source, pd.DataFrame)


class File(DataFrame):
    """
    A data loader for MDS JSON payload files.
    """

    def load(self, source, **kwargs):
        """
        Load MDS data from a file source.

        Parameters:
            source: str, Path
                An mds.files.DataFile compatible JSON file path.

            record_type: str
                The type of MDS data

            table: str
                The name of the database table to insert this data into.

            engine: sqlalchemy.engine.Engine
                The engine used for connections to the database backend.

            Additional keyword arguments are passed-through to DataFrameLoader.load().

        Raise:
            UnexpectedVersionError
                When data is parsed with a version different from what was expected.
        """
        record_type = kwargs.get("record_type")
        version = Version(kwargs.get("version"))

        # read the data file
        _version, df = DataFile(record_type, source).load_dataframe()

        if version and _version != version:
            raise UnexpectedVersionError(_version, version)

        return super().load(df, **kwargs)

    @classmethod
    def can_load(cls, source):
        """
        Returns True if source a valid file source
        """
        try:
            return DataFile(source).file_sources
        except:
            return False


class Records(DataFrame):
    """
    A data loader for MDS record objects:

        {
            "provider_id": "UUID",
            "device_id": "UUID",
            //etc.
        }
    """

    def load(self, source, **kwargs):
        """
        Load data from one or more MDS Provider records.

        Parameters:
            source: dict, list
                One or more dicts of type record_type.

            record_type: str
                The type of MDS data.

            table: str
                The name of the database table to insert this data into.

            engine: sqlalchemy.engine.Engine
                The engine used for connections to the database backend.

            Additional keyword arguments are passed-through to DataFrameLoader.load().
        """
        if isinstance(source, dict):
            source = [source]

        df = pd.DataFrame.from_records(source)
        super().load(df, **kwargs)

    @classmethod
    def can_load(cls, source):
        """
        True if source is one or more MDS Provider record dicts.
        """
        if isinstance(source, dict):
            source = [source]
        return isinstance(source, list) and all([
            isinstance(d, dict) and "provider_id" in d and "device_id" in d
            for d in source
        ])


class Payloads(Records):
    """
    A data loader for MDS payload objects:
        {
            "version": "x.y.z",
            "data": {
                "record_type": [
                    // records here
                ]
            },
            "links": {
            }
        }
    """

    def load(self, source, **kwargs):
        """
        Load data from one or more MDS Provider payloads.

        Parameters:
            source: dict, list
                One or more payload dicts.

            record_type: str
                The type of MDS data.

            table: str
                The name of the database table to insert this data into.

            engine: sqlalchemy.engine.Engine
                The engine used for connections to the database backend.

            Additional keyword arguments are passed-through to DataFrameLoader.load().
        """
        record_type = kwargs.get("record_type")
        version = kwargs.get("version")

        if isinstance(source, dict):
            source = [source]

        payload_key = Schema(record_type).schema_key
        for payload in [p for p in source if payload_key in p["data"]]:
            if version and version != Version(payload["version"]):
                raise UnexpectedVersionError(payload["version"], version)

            records = payload["data"][payload_key]
            super().load(records, **kwargs)

    @classmethod
    def can_load(cls, source):
        """
        True if source is one or more MDS Provider payload dicts.
        """
        """
        True if source is one or more MDS Provider record dicts.
        """
        if isinstance(source, dict):
            source = [source]
        return isinstance(source, list) and all([
            isinstance(d, dict) and "version" in d and "data" in d
            for d in source
        ])


def data_loaders():
    """
    Return a list of all supported data loaders.
    """
    def all_subs(cls):
        return set(cls.__subclasses__()).union(
            [s for c in cls.__subclasses__() for s in all_subs(c)]
        ).union([cls])

    return all_subs(DataFrame)
