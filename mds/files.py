"""
Work with MDS Provider data in JSON files.
"""

import json
import os
import pandas as pd
from pathlib import Path

from .schemas import SCHEMA_TYPES


class ProviderDataFiles():
    """
    Read data from MDS Provider files.
    """

    def __init__(self, record_type=None, *sources, **kwargs):
        """
        Initialize a new ProviderFiles instance.

        Parameters:
            record_type: str, optional
                The type of MDS Provider record ("status_changes" or "trips") to use by default.

            sources: str, Path, list, optional
                One or more paths to (directories containing) MDS payload (JSON) files to read by default.
                Directories are expanded such that all corresponding files within are read.
        """
        self.record_type = None
        self.sources = []

        if record_type:
            if record_type in SCHEMA_TYPES:
                self.record_type = record_type
            else:
                self.sources.append(Path(record_type))

        self.sources.extend([Path(s) if not isinstance(s, Path) else s for s in sources])

    def __record_type_or_raise(self, record_type):
        record_type = record_type or self.record_type

        if not record_type:
            raise ValueError("A record type must be specified.")
        return record_type

    def get_dataframe(self, record_type=None, *sources, **kwargs):
        """
        Reads the contents of MDS payload files into tuples of (Version, DataFrame).

        Parameters:
            record_type: str, optional
                The type of MDS Provider record ("status_changes" or "trips").

            sources: str, list, optional
                One or more paths to (directories containing) MDS payload (JSON) files.
                Directories are expanded such that all corresponding files within are read.

            flatten: bool, optional
                True (default) to flatten the final result from all sources into a single tuple.
                False to keep each result separate.

        Returns:
            tuple
                With flatten=True, a (Version, DataFrame) tuple.

            list
                With flatten=False, a list of (Version, DataFrame) tuples with length equal to the
                total number of payloads across all sources.

        Raises:
            ValueError
                When neither record_type or instance.record_type is provided.

            ValueError
                When flatten=True and a version mismatch is found amongst the data.
        """
        record_type = self.__record_type_or_raise(record_type)

        records = self.get_records(record_type, *sources, flatten=false)

        if len(records) == 0:
            return records

        version = records[0][0]

        if kwargs.pop("flatten", True):
            if not all([v == version for v,_ in records]):
                raise ValueError("Found version mismatch, cannot flatten results.")
            # take the first version, combine each record list
            version, records = results[0][0], [item for _,data in records for item in data]
            return (version, pd.DataFrame.from_records(records))
        else:
            return [(r[0], pd.DataFrame.from_records(r[1])) for r in records]

    def get_payloads(self, record_type=None, *sources, **kwargs):
        """
        Reads the contents of MDS payload files.

        Parameters:
            record_type: str, optional
                The type of MDS Provider record ("status_changes" or "trips").
                By default get payloads of each type.

            sources: str, Path, list, optional
                One or more paths to (directories containing) MDS payload (JSON) files.
                Directories are expanded such that all corresponding files within are read.

            flatten: bool, optional
                True (default) to flatten the final result from all sources into a list of dicts.
                False to keep each result as-is from the source.

        Returns:
            list
                With a single file source, or multiple sources and flatten=True, a list of dicts.
                With multiple sources and flatten=False, a list of the raw contents of each file.

        Raises:
            IndexError
                When no sources have been specified.
        """
        sources = [Path(s) if not isinstance(s, Path) else s for s in sources]

        # record_type is not a schema type, but a data source
        if record_type and record_type not in SCHEMA_TYPES:
            sources.append(Path(record_type))
            record_type = None

        if len(sources) == 0:
            sources.extend(self.sources)

        if len(sources) == 0:
            raise IndexError("There are no sources to read from.")

        record_type = record_type or self.record_type or ""

        # separate into files and directories
        files = [f for f in sources if f.is_file()]
        dirs = [d for d in sources if d.is_dir()]

        # expand into directories
        expanded = [f for ls in [d.glob(f"*{record_type}*") for d in dirs] for f in ls]
        files.extend(expanded)

        # load from each file into a composite list
        data = [json.load(Path(f).open()) for f in files]

        # filter out payloads with non-matching record_type
        if record_type:
            filtered = []
            for payload in data:
                if isinstance(payload, list):
                    filtered.extend(filter(lambda p: record_type in p["data"], payload))
                elif "data" in payload and record_type in payload["data"]:
                    filtered.append(payload)
            data = filtered

        # flatten any sublists
        if kwargs.pop("flatten", True):
            flattened = []
            for payload in data:
                if isinstance(payload, list):
                    flattened.extend(payload)
                else:
                    flattened.append(payload)
            data = flattened

        return data

    def get_records(self, record_type=None, *sources, **kwargs):
        """
        Reads the contents of MDS payload files into tuples of (Version, list).

        Parameters:
            record_type: str, optional
                The type of MDS Provider record ("status_changes" or "trips").

            sources: str, optional
                One or more paths to (directories containing) MDS payload (JSON) files.

            flatten: bool, optional
                True (default) to flatten the final result from all sources into a single list.

                False to keep each result separate.

        Returns:
            tuple
                With flatten=True, a (Version, list) tuple.

            list
                With flatten=False, a list of (Version, list) tuples with length equal to the 
                total number of payloads across all sources.

        Raises:
            ValueError
                When neither record_type or instance.record_type is provided.

            ValueError
                When flatten=True and a version mismatch is found amongst the data.
        """
        record_type = self.__record_type_or_raise(record_type)

        payloads = self.get_payloads(record_type, *sources, flatten=False)

        if len(payloads) < 1:
            return payloads

        # get the version from the initial payload
        if isinstance(payloads[0], list):
            version = payloads[0][0]["version"]
        else:
            version = payloads[0]["version"]

        # collect versions and data from each payload
        results = []
        for payload in payloads:
            if not isinstance(payload, list):
                payload = [payload]
            for page in payload:
                results.append((page["version"], page["data"][record_type]))

        if kwargs.pop("flatten", True):
            if not all([v == version for v,_ in results]):
                raise ValueError("Found version mismatch, cannot flatten results.")
            # take the first version, and unroll each item from each page
            version, records = results[0][0], [item for version,data in results for item in data]
            return (Version(version), records)
        else:
            return [(Version(r[0]), r[1]) for r in results]
