"""
Work with an MDS Provider database.
"""

import json
import mds
from mds.db import sql
from mds.json import read_data_file
import pandas as pd
import psycopg2
import sqlalchemy


TEMP_SC = f"temp_{mds.STATUS_CHANGES}"
TEMP_TRIPS = f"temp_{mds.TRIPS}"


def data_engine(user, password, db, host, port):
    """
    Create a SQLAlchemy engine using the provided connection information.
    """
    url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    return sqlalchemy.create_engine(url)

def load_from_file(src, record_type, engine, table, before_load=None):
    """
    Load the data file of type :record_type: at :src: in the table :table: using the connection
    defined by :engine:.

    :before_load: is an optional callback to pre-process a DataFrame before loading
    it into :table:.
    """
    # read the data file
    _, df = read_data_file(src, record_type)

    # run any pre-processors
    if before_load is not None:
        before_load(df)

    # insert this DataFrame into a new temporary table
    df.to_sql(table, engine, if_exists="replace", index=False)

    # now insert from the temp table to the actual table
    with engine.begin() as conn:
        if record_type == mds.STATUS_CHANGES:
            query = sql.insert_status_changes_from(table)
        elif record_type == mds.TRIPS:
            query = sql.insert_trips_from(table)
        if query is not None:
            conn.execute(query)

def _json_cols_tostring(df, cols):
    """
    For each :cols: in the :df:, convert to a JSON string.
    """
    for col in cols:
        df[col] = df[col].apply(json.dumps)

def load_status_changes(sources, engine):
    """
    Load status_changes data from :sources: using the connection defined by :engine:.
    """
    json_cols = ["event_location"]
    before_load = lambda df: _json_cols_tostring(df, json_cols)

    for src in sources:
        load_from_file(
            src=src,
            record_type=mds.STATUS_CHANGES,
            engine=engine,
            table=TEMP_SC,
            before_load=before_load)

def load_trips(sources, engine):
    """
    Load trips data from :sources: using the connection defined by :engine:.
    """
    json_cols = ["route"]
    before_load = lambda df: _json_cols_tostring(df, json_cols)

    for src in sources:
        load_from_file(
            src=src,
            record_type=mds.TRIPS,
            engine=engine,
            table=TEMP_TRIPS,
            before_load=before_load)

