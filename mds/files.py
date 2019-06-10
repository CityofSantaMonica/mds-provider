"""
Work with MDS Provider data in JSON files.
"""

import csv
import datetime
import hashlib
import json
import os
import pathlib
import urllib

import requests
import pandas as pd

from .encoding import JsonEncoder
from .providers import Provider
from .schemas import SCHEMA_TYPES, STATUS_CHANGES, TRIPS
from .versions import UnexpectedVersionError, Version


class BaseFile():
    """
    Base class for working with Provider files.
    """

    def __init__(self, *sources, **kwargs):
        """
        Parameters:
            sources: str, Path, list, optional
                Zero or more paths to track.
        """
        self._sources = []

        for source in sources:
            if isinstance(source, list):
                self._sources.extend([self._parse(s) for s in source])
            else:
                self._sources.append(self._parse(source))

        self._sources = list(filter(None, self._sources))

    @property
    def file_sources(self):
        """
        True if this instance references one or more valid file sources.
        """
        return all([self._isfile(s) or self._isurl(s) for s in self._sources])

    @classmethod
    def _isdir(cls, source):
        """
        Return True if source is a valid directory that exists.
        """
        path = pathlib.Path(source.path)
        return not cls._isfile(source) and path.is_dir() and path.exists()

    @classmethod
    def _isfile(cls, source):
        """
        Return True if path is a valid file that exists.
        """
        path = pathlib.Path(source.path)
        return not cls._isurl(source) and path.is_file() and path.exists()

    @classmethod
    def _isurl(cls, source):
        """
        Return True if source is a valid URL.
        """
        return source.scheme in ("http", "https") and source.netloc

    @classmethod
    def _parse(cls, source):
        """
        Parse a data file source argument into an urllib.parse.ParseResult instance.
        """
        return urllib.parse.urlparse(str(source)) if source else None


class ConfigFile(BaseFile):
    """
    Work with Provider configuration data in JSON files.
    """

    def __init__(self, path=None, provider=None, **kwargs):
        """
        Parameters:
            path: str, Path, optional
                A path to a configuration file.

            provider: str, UUID, Provider, optional
                An identifier (name, id) for a provider; or a Provider instance. Used to key
                configuration data in a dict.
        """
        super().__init__(path, **kwargs)

        self._config_path = None

        # did we get a single file path or a provider?
        if len(self._sources) == 1 and self._isfile(self._sources[0]):
            self._config_path = pathlib.Path(self._sources[0].path)

        # read from the config file
        if self._config_path:
            config = json.load(self._config_path.open())
            search = []

            # case-insensitive search in config
            if isinstance(provider, Provider):
                search.extend([
                    str(provider.provider_id),
                    provider.provider_name, provider.provider_name.capitalize(),
                    provider.provider_name.lower(), provider.provider_name.upper()
                ])
            elif provider:
                search.extend([
                    provider, provider.lower(), provider.capitalize(), provider.upper()
                ])

            for s in set(search):
                if s in config:
                    config = config.pop(s)
                    break

            for k,v in config.items():
                setattr(self, k, v)

        # set default attributes
        else:
            defaults = [("auth_type", "Bearer"), ("headers", {}), ("version", Version.mds_lower()), ("mds_api_suffix", None)]
            for _field, _default in defaults:
                setattr(self, _field, _default)

        # finally, set from keyword args
        for k,v in kwargs.items():
            setattr(self, k, v)

    def __repr__(self):
        return f"<mds.files.ConfigFile ('{self._config_path}')>"

    def dump(self, path=None, provider=None, **kwargs):
        """
        Convert this instance back into a configuration dict.

        Parameters:
            path: str, Path, optional
                The path to write the configuration data.

            provider: str, UUID, Provider, optional
                An identifier (name, id) for a provider; or a Provider instance. Used to key
                configuration data in a dict.

            Additional keyword arguments are passed-through to json.dump().

        Return:
            dict
                With no path information, return a dict of configuration.

            ConfigFile
                With path information, dump configuration to file path and return this instance.
        """
        dump = vars(self)

        if provider:
            if isinstance(provider, Provider):
                provider = provider.provider_name
            dump = dict([(provider, dump)])

        if path:
            json.dump(dump, pathlib.Path(path).open("w"), cls=JsonEncoder, **kwargs)
            return self

        return dump


class DataFile(BaseFile):
    """
    Work with Provider payload data in JSON files.
    """

    def __init__(self, record_type=None, *sources, **kwargs):
        """
        Parameters:
            record_type: str, optional
                The type of MDS Provider record ("status_changes" or "trips") to use by default.

            sources: str, Path, list, optional
                One or more paths to (directories containing) MDS payload (JSON) files to read by default.
                Directories are expanded such that all corresponding files within are read.

            file_name: str, callable(record_type=str, payloads=list, extension=str, [payload=dict]): str, optional
                A str name for the file; or a function receiving record_type, list of payloads,
                file extension, and optionally a single payload being written, and returns the str
                name for the file.

            ls: callable(sources=list): list, optional
                A function that receives a list of urllib.parse.ParseResult, and returns the
                complete list of file Path objects and URL str to be read.
        """
        super().__init__(*sources, **kwargs)

        self.record_type = None

        if record_type:
            if record_type in SCHEMA_TYPES:
                self.record_type = record_type
            else:
                self._sources.append(self._parse(record_type))

        file_name = kwargs.get("file_name", self._filename)
        if isinstance(file_name, str):
            self.file_name = lambda **kwargs: file_name
        else:
            self.file_name = file_name

        self.ls = kwargs.get("ls", self._ls)

    def __repr__(self):
        return "".join((
            f"<mds.files.DataFile (",
            ", ".join([f"'{s}'" for s in [self.record_type]]),
            ")>"
        ))

    def _default_dir(self):
        """
        Get a default Path object for dumping data files.
        """
        dirs = [s.path for s in self._sources if self._isdir(s)]
        return pathlib.Path(dirs[0]) if len(dirs) == 1 else pathlib.Path(".")

    def _record_type_or_raise(self, record_type):
        """
        Get a valid record_type or raise an exception.
        """
        record_type = record_type or self.record_type

        if record_type in SCHEMA_TYPES:
            return record_type
        raise ValueError(f"A valid record type must be specified. Got {record_type}")

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

            file_name: str, callable(record_type=str, payloads=list, extension=str, [payload=dict]): str, optional
                A str name for the file; or a function receiving record_type, list of payloads,
                file extension, and optionally a single payload being written, and returns the str
                name for the file.

            single_file: bool, optional
                True (default) to write the payloads to a single file using the appropriate data structure.
                False to write each payload as a dict to its own file.

            Additional keyword arguments are passed through to json.dump().

        Return:
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

        output_dir = pathlib.Path(kwargs.pop("output_dir", self._default_dir()))
        single_file = kwargs.pop("single_file", True)

        file_name = kwargs.pop("file_name", self.file_name)
        if isinstance(file_name, str):
            orig_file_name = file_name
            file_name = lambda **kwargs: orig_file_name

        output_dir.mkdir(parents=True, exist_ok=True)

        if single_file:
            version = sources[0]["version"]
            encoder = JsonEncoder(date_format="unix", version=version, **kwargs)

            # generate a file name for the list of payloads
            fname = file_name(record_type=record_type, payloads=sources, extension=".json")
            print(fname)
            path = pathlib.Path(output_dir, fname)

            # dump the single payload or a list of payloads
            if dict_source and len(sources) == 1:
                path.write_text(encoder.encode(sources[0]))
            else:
                path.write_text(encoder.encode(sources))

            return path

        # multi-file
        for payload in sources:
            version = payload["version"]
            encoder = JsonEncoder(date_format="unix", version=version, **kwargs)

            # generate a file name for this payload
            fname = file_name(record_type=record_type, payloads=sources, extension=".json", payload=payload)
            path = pathlib.Path(output_dir, fname)
            if sources.index(payload) > 0 and path.exists():
                # increment the file number
                n = str(sources.index(payload))
                # pad with number of zeros based on how many items in the list
                nz = len(str(len(sources)))
                path = pathlib.Path(str(path).replace(".json", f"_{n.zfill(nz)}.json"))

            # dump the payload dict
            path.write_text(encoder.encode(payload))

        return output_dir

    def load_dataframe(self, record_type=None, *sources, **kwargs):
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

            headers: dict, optional
                A dict of headers to send with requests made to URL paths.
                Could also be a dict mapping an URL path to headers for that path.

            ls: callable(sources=list): list, optional
                A function that receives a list of urllib.parse.ParseResult, and returns the
                complete list of file Path objects and URL str to be read.

        Raise:
            UnexpectedVersionError
                When flatten=True and a version mismatch is found amongst the data.

            ValueError
                When neither record_type or instance.record_type is specified.

        Return:
            tuple (Version, DataFrame)
                With flatten=True, a (Version, DataFrame) tuple.

            list
                With flatten=False, a list of (Version, DataFrame) tuples with length equal to the
                total number of payloads across all sources.
        """
        record_type = self._record_type_or_raise(record_type)
        flatten = kwargs.pop("flatten", True)

        # obtain unmodified records
        kwargs["flatten"] = False
        records = self.load_records(record_type, *sources, **kwargs)

        if len(records) == 0:
            return records

        version = Version(records[0][0])

        if flatten:
            if not all([Version(v) == version for v,_ in records]):
                unexpected = [Version(v) for v,_ in records if Version(v) != version][0]
                raise UnexpectedVersionError(unexpected, version)
            # combine each record list
            records = [item for _,data in records for item in data]
            return version, pd.DataFrame.from_records(records)
        else:
            # list of version, DataFrame tuples
            return [(Version(r[0]), pd.DataFrame.from_records(r[1])) for r in records]

    def load_payloads(self, record_type=None, *sources, **kwargs):
        """
        Reads the contents of MDS payload files.

        Parameters:
            record_type: str, optional
                The type of MDS Provider record ("status_changes" or "trips").
                By default get payloads of each type.

            sources: str, Path, list, optional
                One or more paths to (directories containing) MDS payload (JSON) files.
                Directories are expanded such that all corresponding files within are read.
                URLs pointing to JSON files are also supported.

            flatten: bool, optional
                True (default) to flatten the final result from all sources into a list of dicts.
                False to keep each result as-is from the source.

            headers: dict, optional
                A dict of headers to send with requests made to URL paths.
                Could also be a dict mapping an URL path to headers for that path.

            ls: callable(sources=list): tuple (files: list, urls: list), optional
                A function that receives a list of urllib.parse.ParseResult, and returns
                a tuple of a list of valid files, and a list of valid URLs to be read from.

            Additional keyword arguments are passed through to json.load().

        Raise:
            IndexError
                When no sources have been specified.

        Return:
            list
                With a single file source, or multiple sources and flatten=True, a list of Provider payload dicts.
                With multiple sources and flatten=False, a list of the raw contents of each file.
        """
        sources = [self._parse(s) for s in sources]

        # record_type is not a schema type, but a data source
        if record_type and record_type not in SCHEMA_TYPES:
            sources.append(self._parse(record_type))
            record_type = None

        if len(sources) == 0:
            sources.extend(self._sources)

        if len(sources) == 0:
            raise IndexError("There are no sources to read from.")

        record_type = record_type or self.record_type

        flatten = kwargs.pop("flatten", True)
        headers = kwargs.pop("headers", {})

        # obtain a list of file Paths and URL str to read
        ls = kwargs.pop("ls", self.ls)
        files, urls = ls(sources)

        # load from each file/URL pointer into a composite list
        data = []
        data.extend([json.loads(f.read_text(), **kwargs) for f in files])
        data.extend([requests.get(u, headers=headers.get(u, headers)).json() for u in urls])

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
        if flatten:
            flattened = []
            for payload in data:
                if isinstance(payload, list):
                    flattened.extend(payload)
                else:
                    flattened.append(payload)
            data = flattened

        return data

    def load_records(self, record_type=None, *sources, **kwargs):
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

            headers: dict, optional
                A dict of headers to send with requests made to URL paths.
                Could also be a dict mapping an URL path to headers for that path.

            ls: callable(sources=list): list, optional
                A function that receives a list of urllib.parse.ParseResult, and returns the
                complete list of file Path objects and URL str to be read.

        Raise:
            UnexpectedVersionError
                When flatten=True and a version mismatch is found amongst the data.

            ValueError
                When neither record_type or instance.record_type is provided.

        Return:
            tuple (Version, list)
                With flatten=True, a (Version, list) tuple.

            list
                With flatten=False, a list of (Version, list) tuples with length equal to the 
                total number of payloads across all sources.
        """
        record_type = self._record_type_or_raise(record_type)

        flatten = kwargs.pop("flatten", True)

        # obtain unmodified payloads
        kwargs["flatten"] = False
        payloads = self.load_payloads(record_type, *sources, **kwargs)

        if len(payloads) < 1:
            return payloads

        # get the version from the initial payload
        if isinstance(payloads[0], list):
            version = Version(payloads[0][0]["version"])
        else:
            version = Version(payloads[0]["version"])

        # collect versions and data from each payload
        _payloads = []
        for payload in payloads:
            if not isinstance(payload, list):
                payload = [payload]
            for page in payload:
                _payloads.append((page["version"], page["data"][record_type]))

        if flatten:
            if not all([Version(v) == version for v,_ in _payloads]):
                # find the first non-matching version and raise
                unexpected = [Version(v) for v,_ in _payloads if Version(v) != version][0]
                raise UnexpectedVersionError(unexpected, version)
            # return the version, records tuple
            return version, [item for _,data in _payloads for item in data]
        else:
            # list of version, records tuples
            return [(Version(r[0]), r[1]) for r in _payloads]

    @classmethod
    def _filename(cls, **kwargs):
        """
        Generate a filename from the given parameters.
        """
        record_type = kwargs.get("record_type", None)
        payloads = kwargs.get("payloads", [])
        extension = kwargs.get("extension", ".json")
        payload = kwargs.get("payload", None)

        # is there a single record_type in these payloads that we should use?
        record_types = set([list(p["data"].keys())[0] for p in payloads])
        if record_type is None and len(record_types) == 1:
            record_type = record_types.pop()

        # no record_type specified, generate filename from payload hash
        if record_type is None:
            data = json.dumps(payload or payloads).encode()
            shadigest = hashlib.sha256(data).hexdigest()
            return f"{shadigest[0:7]}{extension}"

        # find time boundaries from the data
        time_key = "event_time" if record_type == STATUS_CHANGES else "start_time"
        times = [d[time_key] for p in payloads for d in p["data"][record_type]]

        if all([isinstance(t, datetime.datetime) for t in times]):
            start = min(times)
            end = max(times)
        else:
            try:
                start = datetime.datetime.utcfromtimestamp(int(min(times)))
                end = datetime.datetime.utcfromtimestamp(int(max(times)))
            except:
                start = datetime.datetime.utcfromtimestamp(int(min(times)) / 1000.0)
                end = datetime.datetime.utcfromtimestamp(int(max(times)) / 1000.0)

        # clip to hour of day, offset if they are the same
        start = datetime.datetime(start.year, start.month, start.day, start.hour)
        end = datetime.datetime(end.year, end.month, end.day, end.hour)
        if start == end:
            end = end + datetime.timedelta(hours=1)

        fmt = "%Y%m%dT%H0000Z"
        providers = set([d["provider_name"] for p in payloads for d in p["data"][record_type]])

        return f"{'_'.join(providers)}_{record_type}_{start.strftime(fmt)}_{end.strftime(fmt)}{extension}"

    @classmethod
    def _ls(cls, sources):
        """
        Create a tuple of lists of valid file Paths and URLs from a list of urllib.parse.ParseResult.
        """
        # separate into files and directories and urls
        files = [pathlib.Path(f.path) for f in sources if cls._isfile(f)]
        dirs = [pathlib.Path(d.path) for d in sources if cls._isdir(d)]
        urls = [urllib.parse.urlunparse(u) for u in sources if cls._isurl(u)]

        # expand into directories
        files.extend([f for ls in [d.glob("*.json") for d in dirs] for f in ls])

        return files, urls
