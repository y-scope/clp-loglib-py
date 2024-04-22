from abc import ABCMeta, abstractmethod
from datetime import datetime
from pathlib import Path
from sys import stderr
from types import TracebackType
from typing import IO, Iterator, List, Match, Optional, Tuple, Type, Union

from clp_ffi_py.ir import FourByteEncoder
from zstandard import ZstdDecompressionReader, ZstdDecompressor

from clp_logging.decoder import CLPDecoder
from clp_logging.protocol import (
    BYTE_ORDER,
    DELIM_DICT,
    DELIM_FLOAT,
    DELIM_INT,
    EOF_CHAR,
    ID_EOF,
    ID_LOGTYPE,
    ID_MASK,
    ID_TIMESTAMP,
    ID_VAR,
    Metadata,
    METADATA_REFERENCE_TIMESTAMP_KEY,
    METADATA_TIMESTAMP_PATTERN_KEY,
    METADATA_TZ_ID_KEY,
    RE_DELIM_VAR,
    RE_DELIM_VAR_UNESCAPE,
    RE_SUB_DELIM_VAR_UNESCAPE,
    VAR_COMPACT_ENCODING,
)

try:
    from zoneinfo import ZoneInfo  # type: ignore[import-not-found, unused-ignore]
except ImportError:
    from backports.zoneinfo import (  # type: ignore[import-not-found, no-redef, unused-ignore]
        ZoneInfo,
    )


class Log:
    """
    An object representing a `logging` record. It is created and returned by
    classes inheriting from `CLPBaseReader`. A `Log` will only contain the
    decoded fields after `_decode` has been called. `Log` objects should be
    created using reader iterators to ensure they are valid.

    :param timestamp_ms: Time in ms since Unix epoch
    :param encoded_logtype: Encoded logtype
    :param encoded_variables: Encoded and untyped variables
    :param variables: Typed and decoded variables
    :param msg: Complete decoded log with no `Formatter` components (`msg`
    field of a `logging.record`)
    :param formatted_msg: Complete decoded log with all `Formatter` components
    """

    def __init__(self) -> None:
        self.timestamp_ms: int
        self.encoded_logtype: bytes
        self.encoded_variables: List[bytes] = []

        self.logtype: str
        self.variables: List[Union[int, float, str]] = []
        self.msg: str
        self.formatted_msg: str

    def __str__(self) -> str:
        return self.formatted_msg

    def _decode(self, timestamp_format: Optional[str], timezone: Optional[ZoneInfo]) -> int:
        """
        Populate the `variables`, `msg`, and `formatted_msg` fields by decoding
        the encoded `encoded_logtype and `encoded_variables`.

        :param timestamp_format: If provided, used by `datetime.strftime` to
        format the timestamp. If `None` then `datetime.isoformat` is used.
        :param timezone: Timezone to use when creating the timestamp from Unix
        epoch time.
        :return: 0 on success, < 0 on error
        """
        var_delim_matchs: List[Match[bytes]] = list(RE_DELIM_VAR.finditer(self.encoded_logtype))
        if not len(self.encoded_variables) == len(var_delim_matchs):
            raise RuntimeError("Number of var delims in logtype does not match stored vars")

        logtype: str = ""
        msg: str = ""
        pos: int = 0
        for i, var_delim_match in enumerate(var_delim_matchs):
            var_start: int = var_delim_match.start()
            logtype += self.encoded_logtype[pos:var_start].decode()
            msg += self.encoded_logtype[pos:var_start].decode()
            pos = var_delim_match.end()

            var_delim: bytes = var_delim_match.group(0)
            var_str: str
            if var_delim == DELIM_DICT:
                var_str = CLPDecoder.decode_dict(self.encoded_variables[i])
                self.variables.append(var_str)
                logtype += "<str>"
            elif var_delim == DELIM_INT:
                var_int: int
                var_int, var_str = CLPDecoder.decode_int(self.encoded_variables[i])
                self.variables.append(var_int)
                logtype += "<int>"
            elif var_delim == DELIM_FLOAT:
                var_float: float
                var_float, var_str = CLPDecoder.decode_float(self.encoded_variables[i])
                self.variables.append(var_float)
                logtype += "<float>"
            else:
                raise RuntimeError("Unknown delimiter")

            msg += var_str
        logtype += self.encoded_logtype[pos:].decode()
        msg += self.encoded_logtype[pos:].decode()
        self.logtype = logtype
        self.msg = msg
        dt: datetime = datetime.fromtimestamp(self.timestamp_ms / 1000, timezone)
        if timestamp_format:
            self.formatted_msg = dt.strftime(timestamp_format) + self.msg
        else:
            self.formatted_msg = dt.isoformat(sep=" ", timespec="milliseconds") + self.msg
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
    :param timestamp_format: If provided, used by `datetime.strftime` to format
    the timestamp. If `None` then `datetime.isoformat` is used.
    :param timezone: Timezone read from CLP IR to use when creating timestamps
    from epoch time
    :param pos: Current position in the `view`
    """

    def __init__(self, timestamp_format: Optional[str], chunk_size: int) -> None:
        """
        Constructor.

        :param timestamp_format: Format optionally provided by user to format
        timestamps from epoch time.
        :param chunk_size: initial size of `_buf` for reading
        """
        self._buf: bytearray = bytearray(chunk_size)
        self.view: memoryview = memoryview(self._buf)
        self.valid_buf_len: int = 0
        self.metadata: Optional[Metadata] = None
        self.last_timestamp_ms: int
        self.timestamp_format: Optional[str] = timestamp_format
        self.timezone: Optional[ZoneInfo]
        self.pos: int

    def read_preamble(self) -> int:
        """
        Try to decode the preamble and populate `metadata`. If the metadata is
        already read it instantly returns. We avoid calling
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

        self.valid_buf_len = self.readinto_buf(0)
        if self.valid_buf_len <= 0:
            raise RuntimeError("readinto_buf for preamble failed")
        try:
            self.metadata, self.pos = CLPDecoder.decode_preamble(self.view, 0)
        except Exception as e:
            if len(self._buf) == self.valid_buf_len:
                raise RuntimeError(
                    "CLPDecoder.decode_preamble failed; CLPReader chunk_size likely too small."
                    f" [self._buf/chunk_size({len(self._buf)}) == self.valid_buf_len"
                    f"({self.valid_buf_len})]"
                ) from e
            else:
                raise
        if self.metadata:
            self.last_timestamp_ms = int(self.metadata[METADATA_REFERENCE_TIMESTAMP_KEY])
            # We do not use the timestamp pattern from the preamble as it may
            # be from other languages and therefore incompatible.
            # self.timestamp_format = self.metadata[METADATA_TIMESTAMP_PATTERN_KEY]
            self.timezone = ZoneInfo(self.metadata[METADATA_TZ_ID_KEY])
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
        Skip all logs with Unix epoch timestamp before `time_ms`.

        After
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
        Read and decode from `view` into `log`. `view` is expected to be past
        the preamble, so `read_preamble` must have been called prior. `pos` is
        only updated when `_buf` and `view` are updated. This allows callers to
        re-read the contents inside `_buf` and `view` if desired.

        :param offset: Position in `view` to begin decoding from
        :param log: `Log` object to store tokens in, if None tokens are dropped
        without further processing and storing
        :return: index read to in `view` (start of next log), 0 on EOF, < 0 on
        error
        """
        while True:
            token: bytes
            while True:
                token_type: int
                token_type, token, pos = CLPDecoder.decode_token(
                    self.view[offset : self.valid_buf_len]
                )
                if token_type == ID_EOF:
                    return 0
                elif token_type == -1:
                    break
                elif token_type < -1:
                    raise RuntimeError(f"Error decoding token: 0x{token.hex()}, type: {token_type}")
                offset += pos

                if log:
                    self._store_token(log, token_type, token)

                # Once we read the timestamp we have completed a log
                token_id: int = token_type & ID_MASK
                if token_id == ID_TIMESTAMP:
                    return offset

            # Shift valid bytes to the start to make room for reading
            # Grow the buffer if more than half is still valid
            valid: int = self.valid_buf_len - offset
            self.view[:valid] = self.view[offset : self.valid_buf_len]
            if valid > len(self._buf) // 2:
                tmp = bytearray(len(self._buf) * 2)
                tmp[:valid] = self.view
                self.view.release()
                self._buf = tmp
                self.view = memoryview(self._buf)
            self.pos = 0
            offset = 0

            ret: int = self.readinto_buf(valid)
            if ret < 0:
                return -1
            self.valid_buf_len = valid + ret

    def _store_token(self, log: Log, token_type: int, token: bytes) -> None:
        """
        Store `token` into the corresponding field in `log` based on the
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
                t = RE_DELIM_VAR_UNESCAPE.sub(RE_SUB_DELIM_VAR_UNESCAPE, t)
            log.encoded_variables.append(t)
        elif token_id == ID_LOGTYPE:
            log.encoded_logtype = RE_DELIM_VAR_UNESCAPE.sub(RE_SUB_DELIM_VAR_UNESCAPE, bytes(token))
        elif token_id == ID_TIMESTAMP:
            delta_ms: int = int.from_bytes(token, BYTE_ORDER, signed=True)
            log.timestamp_ms = self.last_timestamp_ms + delta_ms
            self.last_timestamp_ms = log.timestamp_ms
        else:
            raise RuntimeError(f"Bad token token_id: {token_id}")
        return


class CLPStreamReader(CLPBaseReader):
    """
    Simple stream reader that will decompress the Zstandard stream.

    :param timestamp_format: Format optionally provided by user to format
    timestamps from epoch time.
    :param chunk_size: initial size of `CLPBaseReader._buf` for reading
    """

    def __init__(
        self,
        stream: IO[bytes],
        timestamp_format: Optional[str] = None,
        chunk_size: int = 4096,
        enable_compression: bool = True,
    ) -> None:
        super().__init__(timestamp_format, chunk_size)
        self.stream: IO[bytes] = stream
        self.dctx: Optional[ZstdDecompressor] = None
        self.zstream: Optional[ZstdDecompressionReader] = None
        if enable_compression:
            self.dctx = ZstdDecompressor()
            self.zstream = self.dctx.stream_reader(self.stream, read_across_frames=True)

    def readinto_buf(self, offset: int) -> int:
        """
        Read the CLP IR stream directly or through Zstandard decompression.

        :return: Bytes read (0 for EOF), < 0 on error
        """
        if self.zstream:
            return self.zstream.readinto(self.view[offset:])
        else:
            # see https://github.com/python/typing/issues/659
            return self.stream.readinto(self.view[offset:])  # type: ignore

    def close(self) -> None:
        if self.zstream:
            self.zstream.close()  # type: ignore
        else:
            self.stream.close()


class CLPFileReader(CLPStreamReader):
    """
    Wrapper class that calls `open` for convenience.
    """

    def __init__(
        self,
        fpath: Path,
        timestamp_format: Optional[str] = None,
        chunk_size: int = 4096,
        enable_compression: bool = True,
    ) -> None:
        self.path: Path = fpath
        super().__init__(open(fpath, "rb"), timestamp_format, chunk_size, enable_compression)

    def dump(self) -> None:
        for log in self:
            stderr.write(log.formatted_msg)


class _CLPSegmentStreamingReader:
    """
    Private reader class used to read stream segments for CLP IR/"logs" produced
    by handlers/encoders. The members in this class are used to maintain a
    single operation for segment reading/streaming. The instance of this class
    should not be reused, meaning that for each segment streaming, there should
    be an individual instance of this class associated.

    :param istream: input stream to read from. It should be an abstracted
    uncompressed seekable IR stream, and it should have the method `readinto`
    :param ostream: output stream to write into. It should be an abstracted
    uncompressed IR stream
    :param _buf: Underlying `bytearray` used to read the CLP IR
    :param view: `memoryview` of `bytearray` to allow convenient slicing
    :param offset: position to start reading from, it's 0 by default
    :param metadata: metadata from CLP IR header to construct a legal stream,
    must be provided to resume read from a none-zero offset
    :param last_timestamp_ms: the timestamp of the last successfully streamed
    log message
    :param init_pos: the position to start read the first log event from `_buf`
    :param pos: Current position in the `view`
    :param total_bytes_read: total number of bytes read from istream
    :param total_bytes_write: total number of bytes write into ostream
    :param max_bytes_to_write: maximum bytes to write into ostream. If exceed,
    the streaming will end. It ensures to stream till the last valid log message
    :param _first_stream: to indicate if the stream has executed already.
    Each instance of this class should only stream once
    :param eof_reached: to indicate if the EOF_TOKEN is read from the decoder
    """

    def __init__(
        self,
        istream: IO[bytes],
        ostream: IO[bytes],
        offset: Optional[int] = None,
        max_bytes_to_write: Optional[int] = None,
        metadata: Optional[Metadata] = None,
        chunk_size: int = 4096,
    ) -> None:
        """
        Constructor.

        :param istream: seekable uncompressed input stream
        :param ostream: uncompressed output stream
        :param offset: position to start reading from
        :param max_bytes_to_write: maximum bytes to write into ostream
        :param metadata: metadata from CLP IR header to construct a legal stream
        :param chunk_size: initial size of `_buf` for reading
        """
        self.istream: IO[bytes] = istream
        self.ostream: IO[bytes] = ostream
        self._buf: bytearray = bytearray(chunk_size)
        self.view: memoryview = memoryview(self._buf)
        self.valid_buf_len: int = 0
        self.offset: Optional[int] = offset
        self.metadata: Optional[Metadata] = metadata
        self.last_timestamp_ms: int
        self.init_pos: int = 0
        self.total_bytes_read: int = 0
        self.total_bytes_written: int = 0
        self.max_bytes_to_write: Optional[int] = max_bytes_to_write
        self._first_stream: bool = True
        self.eof_reached: bool = False

    def readinto_buf(self, offset: int) -> int:
        """
        Populate the underlying `_buf`.

        :return: Bytes read (0 for EOF), < 0 on error
        """
        # see https://github.com/python/typing/issues/659
        bytes_read: int = self.istream.readinto(self.view[offset:])  # type:ignore
        self.total_bytes_read += bytes_read
        return bytes_read

    def init_preamble(self) -> Optional[bytearray]:
        """
        Initialize the CLP IR header for the coming read, and initialize
        `last_timestamp_ms`.

        If metadata is not provided, it will attempt to
        parse the header from `_buf` and set `init_pos`.
        :return: CLP IR header represented as a byte array.
        """
        if not self.metadata:
            try:
                self.metadata, self.init_pos = CLPDecoder.decode_preamble(self.view, 0)
            except Exception as e:
                if len(self._buf) == self.valid_buf_len:
                    raise RuntimeError(
                        "CLPDecoder.decode_preamble failed; CLPReader chunk_size likely too small."
                        f" [self._buf/chunk_size({len(self._buf)}) == self.valid_buf_len"
                        f"({self.valid_buf_len})]"
                    ) from e
                else:
                    raise
        if self.metadata:
            self.last_timestamp_ms = int(self.metadata[METADATA_REFERENCE_TIMESTAMP_KEY])
            preamble: bytearray = FourByteEncoder.encode_preamble(
                self.last_timestamp_ms,
                self.metadata[METADATA_TIMESTAMP_PATTERN_KEY],
                self.metadata[METADATA_TZ_ID_KEY],
            )
            return preamble
        return None

    def generate_return_metadata(self) -> Optional[Metadata]:
        """
        Use the latest processed real timestamp to construct a legal CLP IR
        metadata, which can be used to resume read from current position in
        later segment streaming.

        However, if the EOF is reached, None should be returned since the
        terminated read should not be resumed.
        :return: CLP IR metadata header.
        """
        if self.eof_reached:
            return None
        assert self.metadata is not None
        self.metadata[METADATA_REFERENCE_TIMESTAMP_KEY] = str(self.last_timestamp_ms)
        return self.metadata

    def ostream_out_of_write_space(self, len_to_write: int) -> bool:
        """
        Check if the output stream has enough space reserved to write
        `len_to_write` of bytes.

        :return: True if the ostream has no space to write.
        """
        return (
            self.max_bytes_to_write is not None
            and (self.total_bytes_written + len_to_write + 1) > self.max_bytes_to_write
        )

    def stream_ir_segment(self) -> Tuple[int, Optional[Metadata]]:
        """
        Streaming from istream to ostream, starting from `offset`.

        It stops
        either the istream is consumed, or the number of bytes written into the
        ostream exceeds `max_bytes_to_write`. This method will gaurantee to read
        till the last valid log message from the IR.
        :return: How many bytes are consumed from the istream and successfully
        streaming into ostream, along with the metadata to resume streaming from
        the last valid log message.
        """
        if not self._first_stream:
            raise RuntimeError("This object has already streamed.")
        self._first_stream = False

        if self.offset and 0 != self.offset:
            # Seek the input stream to the given position.
            # By default, it should seek from the beginning.
            if not self.metadata:
                raise RuntimeError(
                    "To seek the IR into a none-zero position, a metadata from last read must be"
                    " given."
                )
            self.istream.seek(self.offset)

        bytes_read: int
        bytes_read = self.readinto_buf(0)
        if bytes_read == 0:
            return 0, None
        elif bytes_read < 0:
            raise RuntimeError("Failed to read from input stream.")
        self.valid_buf_len = bytes_read

        preamble = self.init_preamble()
        if not preamble:
            raise RuntimeError("Failed to read initial IR metadata.")

        if self.ostream_out_of_write_space(len(preamble)):
            raise RuntimeError("Output stream limit too small to fit IR metadata.")

        bytes_written = self.ostream.write(preamble)
        if bytes_written <= 0:
            raise RuntimeError("Failed to write into output stream.")
        self.total_bytes_written += bytes_written

        bytes_consumed: int
        offset: int = self.init_pos
        log_buf: bytearray = bytearray(0)
        while True:
            token: bytes
            while True:
                token_type: int
                token_type, token, pos = CLPDecoder.decode_token(
                    self.view[offset : self.valid_buf_len]
                )
                if token_type == ID_EOF:
                    self.eof_reached = True
                    break  # Reach the end of stream
                elif token_type == -1:
                    break  # Populate the buffer and decode again
                elif token_type < -1:
                    raise RuntimeError(f"Error decoding token: 0x{token.hex()}, type: {token_type}")

                log_buf += self.view[offset : offset + pos]
                offset += pos

                token_id: int = token_type & ID_MASK
                if ID_TIMESTAMP == token_id:
                    log_length: int = len(log_buf)
                    if self.ostream_out_of_write_space(log_length):
                        bytes_consumed = (
                            self.total_bytes_read - log_length - (self.valid_buf_len - offset)
                        )
                        self.ostream.write(EOF_CHAR)
                        return bytes_consumed, self.generate_return_metadata()
                    # Increment the last recorded timestamp
                    self.last_timestamp_ms += int.from_bytes(token, BYTE_ORDER, signed=True)
                    bytes_written = self.ostream.write(log_buf)
                    if bytes_written <= 0:
                        raise RuntimeError("Failed to write into out stream.")
                    self.total_bytes_written += bytes_written
                    log_buf.clear()

            if self.eof_reached:
                # For a legal IR stream, if EOF is reached, there should be no
                # extra data appended. In this case, bytes consumed will be
                # the number of bytes read in total. Return None for the
                # metadata because the terminated read should not be resumed.
                self.ostream.write(EOF_CHAR)
                return self.total_bytes_read, self.generate_return_metadata()

            # Shift valid bytes to the start to make room for reading
            # Grow the buffer if more than half is still valid
            valid: int = self.valid_buf_len - offset
            self.view[:valid] = self.view[offset : self.valid_buf_len]
            if valid > len(self._buf) // 2:
                tmp = bytearray(len(self._buf) * 2)
                tmp[:valid] = self.view
                self.view.release()
                self._buf = tmp
                self.view = memoryview(self._buf)

            bytes_read = self.readinto_buf(valid)
            if bytes_read == 0:
                # No more to read. This is the end of the current segment.
                self.ostream.write(EOF_CHAR)
                # Log in the current buffer is not written into ostream.
                bytes_consumed = self.total_bytes_read - len(log_buf) - valid
                return bytes_consumed, self.generate_return_metadata()
            elif bytes_read < 0:
                raise RuntimeError("Failed to read from input stream.")

            offset = 0
            self.valid_buf_len = valid + bytes_read


class CLPSegmentStreaming:
    """
    Wrapper for _CLPSegmentStreamingReader.

    As explained in _CLPSegmentStreamingReader, its members are designed to
    maintain a single read operation and thus not reusable. This class
    encapsulates the actual stream reader class and provides a static method to
    ensure that each individual read operation will have its own instance of
    _CLPSegmentStreamingReader.
    """

    @staticmethod
    def read(
        istream: IO[bytes],
        ostream: IO[bytes],
        offset: Optional[int] = None,
        max_bytes_to_write: Optional[int] = None,
        metadata: Optional[Metadata] = None,
    ) -> Tuple[int, Optional[Metadata]]:
        reader: _CLPSegmentStreamingReader = _CLPSegmentStreamingReader(
            istream=istream,
            ostream=ostream,
            offset=offset,
            max_bytes_to_write=max_bytes_to_write,
            metadata=metadata,
        )
        return reader.stream_ir_segment()
