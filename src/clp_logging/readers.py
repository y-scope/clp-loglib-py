from abc import ABCMeta, abstractmethod
from datetime import datetime, tzinfo
from pathlib import Path
from types import TracebackType
from typing import IO, Iterator, List, Match, Optional, Type, Union

from zstandard import ZstdDecompressor, ZstdDecompressionReader

from clp_logging.decoder import CLPDecoder
from clp_logging.protocol import (
    BYTE_ORDER,
    DELIM_DICT,
    DELIM_FLOAT,
    DELIM_INT,
    ID_EOF,
    ID_LOGTYPE,
    ID_MASK,
    ID_TIMESTAMP,
    ID_VAR,
    Metadata,
    METADATA_REFERENCE_TIMESTAMP_KEY,
    METADATA_TIMESTAMP_PATTERN_KEY,
    METADATA_TZ_ID_KEY,
    RE_UNESCAPE,
    RE_SUB_UNESCAPE,
    VAR_COMPACT_ENCODING,
    RE_DELIM_VAR,
)


class Log:
    """
    An object representing a `logging` record. It is created and returned by
    classes inheriting from `CLPBaseReader`. A `Log` will only contain the
    decoded fields after `_decode` has been called. `Log` objects should be
    created using reader iterators to ensure they are valid.
    :param timestamp_ms: Time in ms since Unix epoch
    :param _logtype: Encoded logtype
    :param _vars: Encoded and untyped variables
    :param variables: Yyped and decoded variables
    :param msg: Complete decoded log with no `Formatter` components (`msg`
    field of a `logging.record`)
    :param log: Complete decoded log with all `Formatter` components
    """

    def __init__(self) -> None:
        self.timestamp_ms: int
        self._logtype: bytes
        self._vars: List[bytes] = []
        self.variables: List[Union[int, float, str]] = []
        self.msg: str
        self.log: str

    def __str__(self) -> str:
        return self.log

    def _decode(self, timestamp_format: str, timezone: Optional[tzinfo]) -> int:
        """
        Populate the `variables`, `msg`, and `log` fields by decoding the
        encoded `_logtype and `_vars`.
        :param timestamp_format: Currently ignored due to compatibility issues
        with other language libraries. (`datatime.isoformat` is always used.)
        :param timezone: Timezone to use when creating the timestamp from Unix
        epoch time.
        :return: 0 on success, < 0 on error
        """
        var_delim_matchs: List[Match[bytes]] = list(RE_DELIM_VAR.finditer(self._logtype))
        if not len(self._vars) == len(var_delim_matchs):
            raise RuntimeError("Number of var delims in logtype does not match stored vars")

        msg: str = ""
        pos: int = 0
        for i, var_delim_match in enumerate(var_delim_matchs):
            var_start: int = var_delim_match.start()
            msg += self._logtype[pos:var_start].decode()

            var_delim: bytes = var_delim_match.group(0)
            var_str: str
            if var_delim == DELIM_DICT:
                var_str = CLPDecoder.decode_dict(self._vars[i])
                self.variables.append(var_str)
            elif var_delim == DELIM_INT:
                var_int: int
                var_int, var_str = CLPDecoder.decode_int(self._vars[i])
                self.variables.append(var_int)
            elif var_delim == DELIM_FLOAT:
                var_float: float
                var_float, var_str = CLPDecoder.decode_float(self._vars[i])
                self.variables.append(var_float)
            else:
                raise RuntimeError("Unknown delimiter")

            msg += var_str
            pos = var_delim_match.end()
        msg += self._logtype[pos:].decode()
        self.msg = msg
        dt: datetime = datetime.fromtimestamp(self.timestamp_ms / 1000, timezone)
        self.log = dt.isoformat(sep=" ", timespec="milliseconds") + self.msg
        return 0


class CLPBaseReader(metaclass=ABCMeta):
    """
    Abstract reader class used to build readers/decoders for CLP IR/"logs"
    produced by handlers/encoders. `readinto_buf` and `close` must be
    implemented by derived readers to correctly managed the underlying `_buf`.
    :param _buf: Underlying `bytearray` used to read the CLP IR
    :param view: `memoryview` of `bytearray` to allow convenient slicing
    :param metadata: Metadata from CLP IR header
    :param last_timestamp_ms:
    :param timestamp_format: Format read from CLP IR to use when creating
    timestamps from epoch time (Currently unused)
    :param timezone: Timezone read from CLP IR to use when creating timestamps
    from epoch time
    :param pos: Current position in the `view`
    """

    def __init__(self, chunk_size: int) -> None:
        """
        Constructor
        :param chunk_size: initial size of `_buf` for reading
        """
        self._buf: bytearray = bytearray(chunk_size)
        self.view: memoryview = memoryview(self._buf)
        self.metadata: Optional[Metadata] = None
        self.last_timestamp_ms: int
        self.timestamp_format: str
        self.timezone: Optional[tzinfo]
        self.pos: int

    def read_preamble(self) -> int:
        """
        Try to decode the preamble and populate `metadata`. If the metadata
        is already read it instantly returns. We avoid calling
        `CLPDecoder.decode_preamble` in `__init_` as `readinto_buf` may block,
        putting unexpected constraints on the user code. For example, any
        stream, file, etc. would need to be readable on a reader's construction
        rather than when the user actually begins to iterate the logs.
        :raises RuntimeError: If `readinto_buf` error or already EOF before
        preamble
        :return: Position in `view`
        """
        if self.metadata:
            return self.pos

        if self.readinto_buf(0) <= 0:
            raise RuntimeError("readinto_buf for preamble failed")
        self.metadata, self.pos = CLPDecoder.decode_preamble(self.view, 0)
        if self.metadata:
            self.last_timestamp_ms = int(self.metadata[METADATA_REFERENCE_TIMESTAMP_KEY])
            self.timestamp_format = self.metadata[METADATA_TIMESTAMP_PATTERN_KEY]
            self.timezone = datetime.strptime(self.metadata[METADATA_TZ_ID_KEY], "%z").tzinfo
        return self.pos

    @abstractmethod
    def readinto_buf(self, offset: int) -> int:
        """
        Abstract method to populate the underlying `_buf`.
        :return: Bytes read (0 for EOF), < 0 on error
        """
        raise NotImplementedError("Readinto_buf must be implemented by derived readers")

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError("Close must be implemented by derived readers")

    def __iter__(self) -> Iterator[Log]:
        if self.read_preamble() <= 0:
            raise RuntimeError("Initialization failed")
        return self

    def __enter__(self) -> Iterator[Log]:
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.close()

    def __next__(self) -> Log:
        log: Log = Log()
        pos: int = self._readinto(self.pos, log)
        if pos == 0:
            raise StopIteration
        elif pos < 0:
            raise RuntimeError("Reading next log failed")
        log._decode(self.timestamp_format, self.timezone)
        self.pos = pos
        return log

    def skip_nlogs(self, n: int = 1) -> int:
        """
        Skip the next `n` log records/events.
        :return: Number of logs skipped
        """
        if self.read_preamble() <= 0:
            raise RuntimeError("Initialization failed")

        for i in range(n):
            pos: int = self._readinto(self.pos, None)
            if pos == 0:
                break
            elif pos < 0:
                raise RuntimeError("Reading next log failed")
            self.pos = pos
        return i

    def skip_to_time(self, time_ms: int) -> int:
        """
        Skip all logs with Unix epoch timestamp before `time_ms`.  After
        being called the next `Log` returned by the reader (e.g. calling
        `__next__`) will return the first log where `Log.timestamp_ms` >=
        `time_ms`
        :return: Number of logs skipped
        """
        if self.read_preamble() <= 0:
            raise RuntimeError("Initialization failed")

        i: int = 0
        while True:
            log: Log = Log()
            pos: int = self._readinto(self.pos, log)
            if pos == 0:
                break
            elif pos < 0:
                raise RuntimeError("Reading next log failed")
            if log.timestamp_ms >= time_ms:
                break
            self.pos = pos
            i += 1
        return i

    def _readinto(self, offset: int, log: Optional[Log]) -> int:
        """
        Read and decode from `view` into `log`. `view` is expected to be
        past the preamble, so `read_preamble` must have been called prior.
        `pos` is only updated when `_buf` and `view` are updated. This allows
        callers to re-read the contents inside `_buf` and `view` if desired.
        :param offset: Position in `view` to begin decoding from
        :param log: `Log` object to store tokens in, if None tokens are dropped
        without further processing and storing
        :return: index read to in `view` (start of next log), 0 on EOF, < 0 on
        error
        """
        variables: List[bytes] = []
        logtype: bytes

        while True:
            while True:
                token_type: int
                token: Optional[bytes]
                token_type, token, pos = CLPDecoder.decode_token(self.view[offset:])
                if token_type == -1:  # Read more
                    break
                elif token_type == 0:  # EOF
                    return 0
                elif token_type < -1 or not token:
                    raise RuntimeError("Error decoding token")

                if log:
                    self._store_token(log, token_type, token)

                offset += pos
                # Once we read the timestamp we have completed a log
                token_id: int = token_type & ID_MASK
                if token_id == ID_TIMESTAMP:
                    return offset
                elif token_id == ID_EOF:
                    return 0

            # Shift valid bytes to the start to make room for reading
            # Grow the buffer if more than half is still valid
            valid: int = len(self.view) - offset
            self.view[:valid] = self.view[offset:]
            if valid > len(self._buf) // 2:
                self.view.release()
                self._buf = bytearray(len(self._buf) * 2)
                self.view = memoryview(self._buf)
            self.pos = 0
            offset = 0

            if self.readinto_buf(valid) <= 0:
                return -1

    def _store_token(self, log: Log, token_type: int, token: bytes) -> None:
        """Store `token` into the corresponding field in `log` based on the
        `token_type`. Bytes in the raw log that match special encoding bytes
        were escaped, so we must unescape any set of bytes copied directly from
        `_buf` (dict variables and logtype).
        :raises RuntimeError: If `token_type & ID_MASK` is invalid
        """
        token_id: int = token_type & ID_MASK
        if token_id == ID_VAR:
            t: bytes = bytes(token)
            if token_type != VAR_COMPACT_ENCODING[0]:
                # remove any escaping done during encoding
                t = RE_UNESCAPE.sub(RE_SUB_UNESCAPE, t)
            log._vars.append(t)
        elif token_id == ID_LOGTYPE:
            log._logtype = RE_UNESCAPE.sub(RE_SUB_UNESCAPE, token)
        elif token_id == ID_TIMESTAMP:
            delta_ms: int = int.from_bytes(token, BYTE_ORDER, signed=True)
            log.timestamp_ms = self.last_timestamp_ms + delta_ms
            self.last_timestamp_ms = log.timestamp_ms
        elif token_id != ID_EOF:
            raise RuntimeError(f"Bad token token_id: {token_id}")
        return


class CLPStreamReader(CLPBaseReader):
    """Simple stream reader that will decompress the Zstandard stream
    :param chunk_size: initial size of `CLPBaseReader._buf` for reading
    """

    def __init__(self, stream: IO[bytes], chunk_size: int = 4096) -> None:
        super().__init__(chunk_size)
        self.stream: IO[bytes] = stream
        self.dctx: ZstdDecompressor = ZstdDecompressor()
        self.zstream: ZstdDecompressionReader = self.dctx.stream_reader(self.stream)

    def readinto_buf(self, offset: int) -> int:
        """Use Zstandard to decompress the CLP IR stream.
        :return: Bytes read (0 for EOF), < 0 on error
        """
        return self.zstream.readinto(self.view[offset:])

    def close(self) -> None:
        self.zstream.close()
        self.stream.close()


class CLPFileReader(CLPStreamReader):
    """Wrapper class that calls `open` for convenience."""

    def __init__(self, fpath: Path) -> None:
        self.path: Path = fpath
        super().__init__(open(fpath, "rb"))
