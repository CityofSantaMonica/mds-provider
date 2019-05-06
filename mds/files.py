"""
Work with MDS Provider data in JSON files.
"""

from datetime import datetime, timedelta
import hashlib
import json
import os
import pandas as pd
from pathlib import Path

from .schemas import STATUS_CHANGES, SCHEMA_TYPES, TRIPS


class ProviderDataFiles():
    """
    Work with data in MDS Provider JSON files.
    """

    def __init__(self, record_type=None, *sources, **kwargs):
        """
        Initialize a new ProviderDataFiles instance.

        Parameters:
            record_type: str, optional
                The type of MDS Provider record ("status_changes" or "trips") to use by default.

            sources: str, Path, list, optional
                One or more paths to (directories containing) MDS payload (JSON) files to read by default.
                Directories are expanded such that all corresponding files within are read.

            file_namer: callable, optional
                A function that receives the the record_type, the payload being written,
                and the full list of payloads, and returns the str file name for for the file.

            file_searcher: callable, optional
                A function that receives a list mixed file/directory Path objects, and returns the
                complete list of file Path objects to be read.
        """
        self.record_type = None
        self.sources = []

        if record_type:
            if record_type in SCHEMA_TYPES:
                self.record_type = record_type
            else:
                self.sources.append(Path(record_type))

        self.sources.extend([Path(s) if not isinstance(s, Path) else s for s in sources])

        self.file_namer = kwargs.get("file_namer", self.__file_namer)
        self.file_searcher = kwargs.get("file_searcher", self.__file_searcher)

    def __default_dir(self):
        dirs = list(filter(lambda path: path.is_dir(), self.sources))
        return dirs[0] if len(dirs) == 1 else Path(".")

    def __record_type_or_raise(self, record_type):
        record_type = record_type or self.record_type

        if not record_type:
            raise ValueError("A record type must be specified.")
        return record_type

    def dump_payloads(self, record_type=None, *payloads, **kwargs):
        """
        Write MDS Provider payloads to JSON files.

        Parameters:
            record_type: str, optional
                The type of MDS Provider record ("status_changes" or "trips").

            payloads: dict, iterable
                One or more MDS Provider payload dicts to write.

            output_dir: str, Path, optional
                The directory to write the files.
                If this instance was initialized with a single directory source, use that by default.
                Otherwise, use the current directory by default.

            file_namer: callable, optional
                A function that receives the the record_type, the payload being written,
                and the full list of payloads, and returns the str file name for the file.

            single_file: bool, optional
                True (default) to write the payloads to a single file using the appropriate data structure.
                False to write each payload as a dict to its own file.

            Additional keyword arguments are passed through to json.dump().

        Returns:
            Path
                With single_file=True, the Path object pointing to the file that was written.
                With single_file=False, the Path object pointing to the directory where files were written.
                None if no files were written.
        """
        sources = []
        # marker indicates if the original incoming source was just a single dict
        dict_source = False

        # not a true record_type, but a data source
        if record_type and record_type not in SCHEMA_TYPES:
            if isinstance(record_type, dict):
                sources.append(record_type)
                dict_source = True
            elif isinstance(record_type, list):
                sources.extend(record_type)
            elif isinstance(record_type, tuple):
                sources.extend(list(record_type))
            record_type = None

        record_type = record_type or self.record_type

        # convert payloads to a flat list of dicts
        if isinstance(payloads, tuple) and len(payloads) == 1:
            payloads = payloads[0]
        if isinstance(payloads, dict):
            payloads = [payloads]
            dict_source = True
        if not isinstance(payloads, list):
            payloads = list(payloads)

        sources.extend(payloads)

        # filter payloads with non-matching record_type
        if record_type in SCHEMA_TYPES:
            sources = [p for p in sources if record_type in p["data"]]

        if len(sources) == 0:
            return None

        output_dir = kwargs.pop("output_dir", self.__default_dir())
        file_namer = kwargs.pop("file_namer", self.file_namer)
        single_file = kwargs.pop("single_file", True)

        Path(output_dir).mkdir(parents=True, exist_ok=True)

        if single_file:
            # generate a file name for the list of payloads
            path = Path(output_dir, file_namer(record_type, sources[0], sources, ".json"))
            # dump the single payload or a list of payloads
            with path.open("w") as fp:
                if dict_source and len(sources) == 1:
                    json.dump(sources[0], fp, **kwargs)
                else:
                    json.dump(sources, fp, **kwargs)
            return path

        for payload in sources:
            # generate a file name for this payload
            path = Path(output_dir, file_namer(record_type, payload, sources, ".json"))
            if sources.index(payload) > 0 and path.exists():
                # increment the file number
                n = str(sources.index(payload))
                # pad with number of zeros based on how many items in the list
                nz = len(str(len(sources)))
                path = Path(str(path).replace(".json", f"_{n.zfill(nz)}.json"))
            # dump the payload dict
            with path.open("w") as fp:
                json.dump(payload, fp, **kwargs)

        return Path(output_dir)

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

            file_searcher: callable, optional
                A function that receives a list mixed file/directory Path objects, and returns the
                complete list of file Path objects to be read.

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
        flatten = kwargs.pop("flatten", True)

        # obtain unmodified records
        kwargs["flatten"] = False
        records = self.get_records(record_type, *sources, **kwargs)

        if len(records) == 0:
            return records

        version = records[0][0]

        if flatten:
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

            file_searcher: callable, optional
                A function that receives a list mixed file/directory Path objects, and returns the
                complete list of file Path objects to be read.

        Returns:
            list
                With a single file source, or multiple sources and flatten=True, a list of Provider payload dicts.
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

        record_type = record_type or self.record_type

        # obtain a list of file Paths to read
        file_searcher = kwargs.get("file_searcher", self.file_searcher)
        files = file_searcher(sources)

        # load from each file into a composite list
        data = [json.load(f.open()) for f in files]

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

            file_searcher: callable, optional
                A function that receives a list mixed file/directory Path objects, and returns the
                complete list of file Path objects to be read.

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

        flatten = kwargs.pop("flatten", True)

        # obtain unmodified payloads
        kwargs["flatten"] = False
        payloads = self.get_payloads(record_type, *sources, **kwargs)

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

        if flatten:
            if not all([v == version for v,_ in results]):
                raise ValueError("Found version mismatch, cannot flatten results.")
            # take the first version, and unroll each item from each page
            version, records = results[0][0], [item for version,data in results for item in data]
            return (Version(version), records)
        else:
            return [(Version(r[0]), r[1]) for r in results]

    @staticmethod
    def __file_namer(record_type, payload, payloads, extension):
        """
        Generate a filename from the given parameters.
        """
        # is there a single record_type in these payloads that we should use?
        record_types = set([list(p["data"].keys())[0] for p in payloads])
        if record_type is None and len(record_types) == 1:
            record_type = record_types.pop()

        # no record_type specified, generate filename from payload hash
        if record_type is None:
            data = json.dumps(payload).encode()
            shadigest = hashlib.sha256(data).hexdigest()
            return f"{shadigest[0:7]}{extension}"

        # find the min/max times from the data
        time_key = "event_time" if record_type == STATUS_CHANGES else "start_time"
        times = [int(d[time_key]) for p in payloads for d in p["data"][record_type]]
        try:
            start = datetime.utcfromtimestamp(min(times))
            end = datetime.utcfromtimestamp(max(times))
        except:
            start = datetime.utcfromtimestamp(min(times) / 1000)
            end = datetime.utcfromtimestamp(max(times) / 1000)

        # clip to hour of day, offset if they are the same
        start = datetime(start.year, start.month, start.day, start.hour)
        end =   datetime(end.year, end.month, end.day, end.hour)
        if start == end:
            end = end + timedelta(hours=1)

        fmt = "%Y%m%dT%H0000Z"
        providers = set([d["provider_name"] for p in payloads for d in p["data"][record_type]])

        return f"{'_'.join(providers)}_{record_type}_{start.strftime(fmt)}_{end.strftime(fmt)}{extension}"

    @staticmethod
    def __file_searcher(sources):
        """
        Create a list of file Paths from a mix of file/directory sources.
        """
        # separate into files and directories
        files = [f for f in sources if f.is_file() and f.suffix.lower() == ".json"]
        dirs = [d for d in sources if d.is_dir()]

        # expand into directories
        files.extend([f for ls in [d.glob("*.json") for d in dirs] for f in ls])

        return files
