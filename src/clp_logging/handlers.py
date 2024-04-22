import logging
import os
import socket
import sys
import time
from abc import ABCMeta, abstractmethod
from math import floor
from pathlib import Path
from queue import Empty, Queue
from signal import SIGINT, signal, SIGTERM
from threading import Thread, Timer
from types import FrameType
from typing import Callable, ClassVar, Dict, IO, Optional, Tuple, Union

import tzlocal
from clp_ffi_py.ir import FourByteEncoder
from zstandard import FLUSH_FRAME, ZstdCompressionWriter, ZstdCompressor

from clp_logging.protocol import (
    BYTE_ORDER,
    EOF_CHAR,
    INT_MAX,
    INT_MIN,
    SIZEOF_INT,
    UINT_MAX,
    ULONG_MAX,
)

# TODO: lock writes to zstream if GIL ever goes away
# Note: no need to quote "Queue[Tuple[int, bytes]]" in python 3.9

DEFAULT_LOG_FORMAT: str = " %(levelname)s %(name)s %(message)s"
WARN_PREFIX: str = " [WARN][clp_logging]"


def _init_timeinfo(fmt: Optional[str], tz: Optional[str]) -> Tuple[str, str]:
    """
    Return the timestamp format and timezone (in TZID format) that should be
    used. If not specified by the user, this function will choose default values
    to use.

    The timestamp format (`fmt`) defaults to a format for the Java readers, due
    to compatibility issues between language time libraries.
    (`datatime.isoformat` is always used for the timestamp format in python
    readers.)

    The timezone format (`tz`) first defaults to the system local timezone.

    If that fails it will default to UTC. In the future sanitization
    of user input should also go here.

    :param fmt: Timestamp format written in preamble to be used when generating
    the logs with a reader.
    :param tz: Timezone in TZID format written in preamble to be used when
    generating the timestamp from Unix epoch time.
    :return: Tuple of timestamp format string and timezone in TZID format.
    """
    if not fmt:
        fmt = "yyyy-MM-d H:m:s.A"
    if not tz:
        try:
            tz = tzlocal.get_localzone_name()
        except Exception:
            tz = "UTC"

    return fmt, tz


def _encode_log_event(msg: str, last_timestamp_ms: int) -> Tuple[bytearray, int]:
    """
    Encodes the log event with the input log message and reference timestamp.

    :param msg: Input log message.
    :param last_timestamp_ms: The timestamp of the last log event. Will be used
        to calculate the timestamp delta.
    :return: A tuple of the encoded log event and the associated timestamp.
    """
    timestamp_ms: int = floor(time.time() * 1000)
    clp_msg: bytearray = FourByteEncoder.encode_message_and_timestamp_delta(
        timestamp_ms - last_timestamp_ms, msg.encode()
    )
    return clp_msg, timestamp_ms


class CLPBaseHandler(logging.Handler, metaclass=ABCMeta):
    def __init__(self) -> None:
        super().__init__()
        self.formatter: logging.Formatter = logging.Formatter(DEFAULT_LOG_FORMAT)

    # override
    def setFormatter(self, fmt: Optional[logging.Formatter]) -> None:
        """
        Check user `fmt` and remove any timestamp token to avoid double printing
        the timestamp.
        """
        if not fmt or not fmt._fmt:
            return

        fmt_str: str = fmt._fmt
        if "asctime" in fmt_str:
            found: Optional[str] = None
            if fmt_str.startswith("%(asctime)s "):
                found = "%(asctime)s"
            elif fmt_str.startswith("{asctime} "):
                found = "{asctime}"
            elif fmt_str.startswith("${asctime} "):
                found = "${asctime}"

            if found:
                fmt._fmt = fmt_str.replace(found, "")
                self._warn(f"replacing '{found}' with clp_logging timestamp format")
            else:
                fmt._fmt = DEFAULT_LOG_FORMAT
                self._warn(f"replacing '{fmt_str}' with '{DEFAULT_LOG_FORMAT}'")
        else:
            fmt._fmt = " " + fmt_str
            self._warn("prepending clp_logging timestamp to formatter")

        fmt._style = fmt._style.__class__(fmt._fmt)
        self.formatter = fmt

    def _warn(self, msg: str) -> None:
        self._write(logging.WARN, f"{WARN_PREFIX} {msg}\n")

    @abstractmethod
    def _write(self, loglevel: int, msg: str) -> None:
        raise NotImplementedError("_write must be implemented by derived handlers")

    # override
    def emit(self, record: logging.LogRecord) -> None:
        """
        Override `logging.Handler.emit` in base class to ensure
        `logging.Handler.handleError` is always called and avoid requiring a
        `logging.LogRecord` to call internal writing functions.
        """
        msg: str = self.format(record) + "\n"
        try:
            self._write(record.levelno, msg)
        except Exception:
            self.handleError(record)


class CLPLogLevelTimeout:
    """
    A configurable timeout feature based on logging level that can be added to a
    CLPHandler. To schedule a timeout, two timers are calculated with each new
    log event. There is no distinction between the timer that triggers a timeout
    and once a timeout occurs both timers are reset. A timeout will always flush
    the zstandard frame and then call a user supplied function (`timeout_fn`).
    An additional timeout is always triggered on closing the logging handler.

    The two timers are implemented using `threading.Timer`. Each timer utilizes
    a map that associates each log level to a time delta (in milliseconds).
    Both the time deltas and the log levels are user configurable.
    The timers:
        - Hard timer: Represents the maximum elapsed time between generating a
          log event and triggering a timeout. The hard timer can only decrease
          towards a timeout occuring. It will decrease if a log level's time
          delta would schedule the timeout sooner. In other words, seeing a
          more important log level will cause a timeout to occur sooner. It is
          not possible for the hard timer to increase (a log whose level's
          delta would schedule a timeout after the current hard timer will not
          update the hard timer).
        - Soft timer: Represents the maximum elapsed time to wait for a new log
          event to be generated before triggering a timeout. Seeing a new log
          event will always cause the soft timer to be recalculated. The delta
          used to calculate the new soft timer is the lowest delta seen since
          the last timeout. Therefore, if we've seen a log level with a low
          delta, that delta will continue to be used to calculate the soft
          timer until a timeout occurs.
    """

    # delta times in milliseconds
    # note: logging.FATAL == logging.CRITICAL and
    #       logging.WARN == logging.WARNING
    _HARD_TIMEOUT_DELTAS: Dict[int, int] = {
        logging.FATAL: 5 * 60 * 1000,
        logging.ERROR: 5 * 60 * 1000,
        logging.WARN: 10 * 60 * 1000,
        logging.INFO: 30 * 60 * 1000,
        logging.DEBUG: 30 * 60 * 1000,
    }
    _SOFT_TIMEOUT_DELTAS: Dict[int, int] = {
        logging.FATAL: 5 * 1000,
        logging.ERROR: 10 * 1000,
        logging.WARN: 15 * 1000,
        logging.INFO: 3 * 60 * 1000,
        logging.DEBUG: 3 * 60 * 1000,
    }

    def __init__(
        self,
        timeout_fn: Callable[[], None],
        hard_timeout_deltas: Dict[int, int] = _HARD_TIMEOUT_DELTAS,
        soft_timeout_deltas: Dict[int, int] = _SOFT_TIMEOUT_DELTAS,
    ) -> None:
        """
        Constructs a `CLPLogLevelTimeout` object which can be added to a
        CLPHandler to enable the timeout feature. `timeout_fn` is the only
        required parameter, but can be an empty function. Functionally, this
        will ensure a zstandard frame is flushed periodically. See
        `_HARD_TIMEOUT_DELTAS` and `_SOFT_TIMEOUT_DELTAS` for the default
        timeout values in milliseconds.

        :param timeout_fn: A user function that will be called when a timeout
            (hard or soft) occurs.
        :param hard_timeout_deltas: A dictionary, mapping a log level (as an
            int) to the maximum elapsed time (in milliseconds) between
            generating a log event and triggering a timeout.
        :param soft_timeout_deltas: A dictionary, mapping a log level (as an
            int) to the maximum elapsed time to wait (in milliseconds) for a new
            log event to be generated before triggering a timeout.
        """
        self.hard_timeout_deltas: Dict[int, int] = hard_timeout_deltas
        self.soft_timeout_deltas: Dict[int, int] = soft_timeout_deltas
        self.timeout_fn: Callable[[], None] = timeout_fn
        self.next_hard_timeout_ts: int = ULONG_MAX
        self.min_soft_timeout_delta: int = ULONG_MAX
        self.ostream: Optional[Union[ZstdCompressionWriter, IO[bytes]]] = None
        self.hard_timeout_thread: Optional[Timer] = None
        self.soft_timeout_thread: Optional[Timer] = None

    def set_ostream(self, ostream: Union[ZstdCompressionWriter, IO[bytes]]) -> None:
        self.ostream = ostream

    def timeout(self) -> None:
        """
        Wraps the call to the user supplied `timeout_fn` ensuring that any
        existing timeout threads are cancelled, `next_hard_timeout_ts` and
        `min_soft_timeout_delta` are reset, and the zstandard frame is flushed.
        """
        if self.hard_timeout_thread:
            self.hard_timeout_thread.cancel()
        if self.soft_timeout_thread:
            self.soft_timeout_thread.cancel()
        self.next_hard_timeout_ts = ULONG_MAX
        self.min_soft_timeout_delta = ULONG_MAX

        if self.ostream:
            if isinstance(self.ostream, ZstdCompressionWriter):
                self.ostream.flush(FLUSH_FRAME)
            else:
                self.ostream.flush()
        self.timeout_fn()

    def update(self, loglevel: int, log_timestamp_ms: int, log_fn: Callable[[str], None]) -> None:
        """
        Carries out the logic to schedule the next timeout based on the current
        log.

        :param loglevel: The log level (verbosity) of the current log.
        :param log_timestamp_ms: The timestamp in milliseconds of the current
            log.
        :param logfn: A function used for internal logging by the library. This
            allows us to correctly log through the handler itself rather than
            just printing to stdout/stderr.
        """
        hard_timeout_delta: int
        if loglevel not in self.hard_timeout_deltas:
            log_fn(
                f"{WARN_PREFIX} log level {loglevel} not in self.hard_timeout_deltas; defaulting"
                " to _HARD_TIMEOUT_DELTAS[logging.INFO].\n"
            )
            hard_timeout_delta = CLPLogLevelTimeout._HARD_TIMEOUT_DELTAS[logging.INFO]
        else:
            hard_timeout_delta = self.hard_timeout_deltas[loglevel]

        new_hard_timeout_ts: int = log_timestamp_ms + hard_timeout_delta
        if new_hard_timeout_ts < self.next_hard_timeout_ts:
            if self.hard_timeout_thread:
                self.hard_timeout_thread.cancel()
            self.hard_timeout_thread = Timer(new_hard_timeout_ts / 1000 - time.time(), self.timeout)
            self.hard_timeout_thread.setDaemon(True)
            self.hard_timeout_thread.start()
            self.next_hard_timeout_ts = new_hard_timeout_ts

        soft_timeout_delta: int
        if loglevel not in self.soft_timeout_deltas:
            log_fn(
                f"{WARN_PREFIX} log level {loglevel} not in self.soft_timeout_deltas; defaulting"
                " to _SOFT_TIMEOUT_DELTAS[logging.INFO].\n"
            )
            soft_timeout_delta = CLPLogLevelTimeout._SOFT_TIMEOUT_DELTAS[logging.INFO]
        else:
            soft_timeout_delta = self.soft_timeout_deltas[loglevel]

        if soft_timeout_delta < self.min_soft_timeout_delta:
            self.min_soft_timeout_delta = soft_timeout_delta

        new_soft_timeout_ms: int = log_timestamp_ms + soft_timeout_delta
        if self.soft_timeout_thread:
            self.soft_timeout_thread.cancel()
        self.soft_timeout_thread = Timer(new_soft_timeout_ms / 1000 - time.time(), self.timeout)
        self.soft_timeout_thread.setDaemon(True)
        self.soft_timeout_thread.start()


class CLPSockListener:
    """
    Server that listens to a named Unix domain socket for `CLPSockHandler`
    instances writing CLP IR.

    Can be started by explicitly calling `CLPSockListener.fork` or by
    instantiating a `CLPSockHandler` with `create_listener=True`. Can be stopped
    by either sending the process SIGINT, SIGTERM, or `EOF_CHAR`.
    """

    _signaled: ClassVar[bool] = False

    @staticmethod
    def _try_bind(sock: socket.socket, sock_path: Path) -> int:
        """
        Tries to bind the given socket to the given path.

        Bind will fail if the socket file exists due to:
            a. Another listener currently exists and is running
                -> try to connect to it
            b. A listener existed in the past, but is now gone
                -> recovery unsupported
        :return: 0 on success or < 0 on failure. 0: bind succeeds, -1: connect
        succeeds, -2: nothing worked
        """
        try:
            sock.bind(str(sock_path))
            return 0
        except OSError:
            pass

        test_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        if test_sock.connect_ex(str(sock_path)) == 0:
            test_sock.close()
            sock.close()
            return -1
        else:
            return -2

    @staticmethod
    def _exit_handler(signum: int, frame: Optional[FrameType]) -> None:
        _signaled = True

    @staticmethod
    def _handle_client(
        conn: socket.socket,
        log_queue: "Queue[Tuple[int, bytes]]",
    ) -> int:
        """
        Continuously reads from an individual `CLPSockHandler` and sends the
        received messages to the aggregator thread.

        :param conn: Client socket where encoded messages and their length are
        received.
        :param log_queue: Queue with `CLPSockListener._aggregator` thread to
        write encoded messages.
        """
        int_buf: bytearray = bytearray(SIZEOF_INT)
        while not CLPSockListener._signaled:
            try:
                read: int = conn.recv_into(int_buf, SIZEOF_INT)
                assert read == SIZEOF_INT
                size: int = int.from_bytes(int_buf, BYTE_ORDER)
                if size == 0:
                    log_queue.put((0, EOF_CHAR))
                    break
                read = conn.recv_into(int_buf, SIZEOF_INT)
                assert read == SIZEOF_INT
                loglevel: int = int.from_bytes(int_buf, BYTE_ORDER)
                buf: bytearray = bytearray(size)
                view: memoryview = memoryview(buf)
                i: int = 0
                while i < size:
                    read = conn.recv_into(view[i:], size - i)
                    if read == 0:
                        raise OSError("handler conn.recv_into returned 0 before finishing")
                    i += read
                log_queue.put((loglevel, buf))
            except socket.timeout:  # TODO replaced with TimeoutError in python 3.10
                pass
            except OSError:
                conn.close()
                raise
        conn.close()
        return 0

    @staticmethod
    def _aggregator(
        log_path: Path,
        log_queue: "Queue[Tuple[int, bytes]]",
        timestamp_format: Optional[str],
        timezone: Optional[str],
        timeout: int,
        enable_compression: bool,
        loglevel_timeout: Optional[CLPLogLevelTimeout] = None,
    ) -> int:
        """
        Continuously receive encoded messages from
        `CLPSockListener._handle_client` threads and write them to a Zstandard
        stream.

        :param log_path: Path to log file and used to derive socket name.
        :param log_queue: Queue with `CLPSockListener._handle_client` threads
        to write encoded messages.
        :param timestamp_format: Timestamp format written in preamble to be
        used when generating the logs with a reader.
        :param timezone: Timezone written in preamble to be used when
        generating the timestamp from Unix epoch time.
        :param timeout: timeout in seconds to prevent `Queue.get` from never
        :param enable_compression: use the zstd compress if setting to True
        returning and not closing properly on signal/EOF_CHAR.
        :return: 0 on successful exit
        """
        cctx: ZstdCompressor = ZstdCompressor()
        timestamp_format, timezone = _init_timeinfo(timestamp_format, timezone)
        last_timestamp_ms: int = floor(time.time() * 1000)  # convert to ms and truncate

        with log_path.open("ab") as log:
            # Since the compression may be disabled, context manager is not used
            ostream: Union[ZstdCompressionWriter, IO[bytes]] = (
                cctx.stream_writer(log) if enable_compression else log
            )

            if loglevel_timeout:
                loglevel_timeout.set_ostream(ostream)

            def log_fn(msg: str) -> None:
                nonlocal last_timestamp_ms
                buf: bytearray
                buf, last_timestamp_ms = _encode_log_event(msg, last_timestamp_ms)
                ostream.write(buf)

            ostream.write(
                FourByteEncoder.encode_preamble(last_timestamp_ms, timestamp_format, timezone)
            )
            while not CLPSockListener._signaled:
                msg: bytes
                try:
                    loglevel, msg = log_queue.get(timeout=timeout)
                except Empty:
                    continue
                if msg == EOF_CHAR:
                    break
                buf: bytearray = bytearray(msg)
                timestamp_ms: int = floor(time.time() * 1000)
                timestamp_buf: bytearray = FourByteEncoder.encode_timestamp_delta(
                    timestamp_ms - last_timestamp_ms
                )
                last_timestamp_ms = timestamp_ms
                if loglevel_timeout:
                    loglevel_timeout.update(loglevel, last_timestamp_ms, log_fn)
                buf += timestamp_buf
                ostream.write(buf)
            if loglevel_timeout:
                loglevel_timeout.timeout()
            ostream.write(EOF_CHAR)

            if enable_compression:
                # Since we are not using context manager, the ostream should be
                # explicitly closed.
                ostream.close()
        # tell _server to exit
        CLPSockListener._signaled = True
        return 0

    @staticmethod
    def _server(
        parent_fd: int,
        log_path: Path,
        sock_path: Path,
        timestamp_format: Optional[str],
        timezone: Optional[str],
        timeout: int,
        enable_compression: bool,
        loglevel_timeout: Optional[CLPLogLevelTimeout] = None,
    ) -> int:
        """
        The `CLPSockListener` server function run in a new process. Writes 1
        byte back to parent process for synchronization.

        :param parent_fd: Used to communicate to parent `CLPSockHandler`
        process that `_try_bind` has finished.
        :param log_path: Path to log file.
        :param sock_path: Path to socket file.
        :param timestamp_format: Timestamp format written in preamble to be
        used when generating the logs with a reader.
        :param timezone: Timezone written in preamble to be used when
        generating the timestamp from Unix epoch time.
        :param timeout: timeout in seconds to prevent block operations from
        never returning and not closing properly on signal/EOF_CHAR
        :return: 0 on successful exit, -1 if `CLPSockListener._try_bind` fails
        """
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        log_queue: "Queue[Tuple[int, bytes]]" = Queue()
        ret: int = CLPSockListener._try_bind(sock, sock_path)
        sock.listen()
        os.write(parent_fd, b"\x00")
        if ret < 0:
            return ret

        signal(SIGINT, CLPSockListener._exit_handler)
        signal(SIGTERM, CLPSockListener._exit_handler)

        Thread(
            target=CLPSockListener._aggregator,
            args=(
                log_path,
                log_queue,
                timestamp_format,
                timezone,
                timeout,
                enable_compression,
                loglevel_timeout,
            ),
            daemon=False,
        ).start()

        while not CLPSockListener._signaled:
            conn: socket.socket
            addr: Tuple[str, int]
            try:
                conn, addr = sock.accept()
                conn.settimeout(timeout)
                Thread(
                    target=CLPSockListener._handle_client, args=(conn, log_queue), daemon=False
                ).start()
            except socket.timeout:
                pass
        sock.close()
        sock_path.unlink()
        return 0

    @staticmethod
    def fork(
        log_path: Path,
        timestamp_format: Optional[str],
        timezone: Optional[str],
        timeout: int,
        enable_compression: bool,
        loglevel_timeout: Optional[CLPLogLevelTimeout] = None,
    ) -> int:
        """
        Fork a process running `CLPSockListener._server` and use `os.setsid()`
        to give it another session id (and process group id). The parent will
        not return until the forked listener has either bound the socket or
        finished trying to.

        :param log_path: Path to log file and used to derive socket name.
        :param timestamp_format: Timestamp format written in preamble to be used
            when generating the logs with a reader.
        :param timezone: Timezone written in preamble to be used when generating
            the timestamp from Unix epoch time.
        :param timeout: timeout in seconds to prevent block operations from
            never returning and not closing properly on signal/EOF_CHAR
        :return: child pid
        """
        sock_path: Path = log_path.with_suffix(".sock")
        rfd: int
        wfd: int
        rfd, wfd = os.pipe()
        pid: int = os.fork()
        if pid == 0:
            os.setsid()
            sys.exit(
                CLPSockListener._server(
                    wfd,
                    log_path,
                    sock_path,
                    timestamp_format,
                    timezone,
                    timeout,
                    enable_compression,
                    loglevel_timeout,
                )
            )
        else:
            os.read(rfd, 1)
        return pid


class CLPSockHandler(CLPBaseHandler):
    """
    Similar to `logging.Handler.SocketHandler`, but the log is written to the
    socket in CLP IR encoding rather than bytes.

    It is also simplified to only work with Unix domain sockets. The log is
    written to a socket named `log_path.with_suffix(".sock")`
    """

    def __init__(
        self,
        log_path: Path,
        create_listener: bool = False,
        timestamp_format: Optional[str] = None,
        timezone: Optional[str] = None,
        timeout: int = 2,
        enable_compression: bool = True,
        loglevel_timeout: Optional[CLPLogLevelTimeout] = None,
    ) -> None:
        """
        Constructor method that optionally spawns a `CLPSockListener`.

        :param log_path: Path to log file written by `CLPSockListener` used to
        derive socket name.
        :param create_listener: If true and the handler could not connect to an
        existing listener, CLPSockListener.fork is used to try and spawn one.
        This is safe to be used by concurrent `CLPSockHandler` instances.
        :param timestamp_format: Timestamp format written in preamble to be
        used when generating the logs with a reader. (Only used when creating a
        listener.)
        :param timezone: Timezone written in preamble to be used when
        generating the timestamp from Unix epoch time. (Only used when creating
        a listener.)
        :param timeout: timeout in seconds to prevent blocking operations from
        never returning and not closing properly on signal/EOF_CHAR
        """
        super().__init__()
        self.sock_path: Path = log_path.with_suffix(".sock")
        self.closed: bool = False
        self.sock: socket.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.listener_pid: int = 0

        if self.sock.connect_ex(str(self.sock_path)) != 0:
            if create_listener:
                self.listener_pid = CLPSockListener.fork(
                    log_path,
                    timestamp_format,
                    timezone,
                    timeout,
                    enable_compression,
                    loglevel_timeout,
                )

            # If we fail to connect again, the listener failed to resolve any
            # issues, so we raise an exception as there is nothing new to try
            try:
                self.sock.connect(str(self.sock_path))
            except OSError:
                self.sock.close()
                raise

    # override
    def _write(self, loglevel: int, msg: str) -> None:
        try:
            if self.closed:
                raise RuntimeError("Socket already closed")
            clp_msg: bytearray = FourByteEncoder.encode_message(msg.encode())
            size: int = len(clp_msg)
            if size > UINT_MAX:
                raise NotImplementedError(
                    "Encoded message longer than UINT_MAX currently unsupported"
                )
            loglevelb: bytes = loglevel.to_bytes(SIZEOF_INT, BYTE_ORDER)
            if loglevel > INT_MAX or loglevel < INT_MIN:
                raise NotImplementedError("Log level larger than signed INT currently unsupported")
            sizeb: bytes = size.to_bytes(SIZEOF_INT, BYTE_ORDER)
            self.sock.sendall(sizeb)
            self.sock.sendall(loglevelb)
            self.sock.sendall(clp_msg)
        except Exception as e:
            self.sock.close()
            raise e

    # override
    def handleError(self, record: logging.LogRecord) -> None:
        self.sock.close()
        logging.Handler.handleError(self, record)

    # override
    def close(self) -> None:
        self.acquire()
        try:
            self.sock.close()
            super().close()
        finally:
            self.release()
        self.closed = True

    def stop_listener(self) -> None:
        try:
            self.sock.send((0).to_bytes(SIZEOF_INT, BYTE_ORDER))
        except Exception:
            sock: socket.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.connect(str(self.sock_path))
            sock.send((0).to_bytes(SIZEOF_INT, BYTE_ORDER))
            sock.close()
        self.close()


class CLPStreamHandler(CLPBaseHandler):
    """
    Similar to `logging.StreamHandler`, but the log is written to `stream` in
    CLP IR encoding rather than bytes or a string.

    :param stream: Output stream of bytes to write CLP encoded log messages to
    """

    def init(self, stream: IO[bytes]) -> None:
        self.cctx: ZstdCompressor = ZstdCompressor()
        self.ostream: Union[ZstdCompressionWriter, IO[bytes]] = (
            self.cctx.stream_writer(stream) if self.enable_compression else stream
        )
        self.last_timestamp_ms: int = floor(time.time() * 1000)  # convert to ms and truncate
        self.ostream.write(
            FourByteEncoder.encode_preamble(
                self.last_timestamp_ms, self.timestamp_format, self.timezone
            )
        )

    def __init__(
        self,
        stream: Optional[IO[bytes]],
        enable_compression: bool = True,
        timestamp_format: Optional[str] = None,
        timezone: Optional[str] = None,
        loglevel_timeout: Optional[CLPLogLevelTimeout] = None,
    ) -> None:
        super().__init__()
        self.closed: bool = False
        self.enable_compression: bool = enable_compression
        if stream is None:
            stream = sys.stderr.buffer
        self.stream: IO[bytes] = stream
        self.timestamp_format: str
        self.timezone: str
        self.timestamp_format, self.timezone = _init_timeinfo(timestamp_format, timezone)
        self.init(self.stream)

        if loglevel_timeout:
            loglevel_timeout.set_ostream(self.ostream)
        self.loglevel_timeout = loglevel_timeout

    def _direct_write(self, msg: str) -> None:
        if self.closed:
            raise RuntimeError("Stream already closed")
        clp_msg: bytearray
        clp_msg, self.last_timestamp_ms = _encode_log_event(msg, self.last_timestamp_ms)
        self.ostream.write(clp_msg)

    # override
    def _write(self, loglevel: int, msg: str) -> None:
        if self.closed:
            raise RuntimeError("Stream already closed")
        clp_msg: bytearray
        clp_msg, self.last_timestamp_ms = _encode_log_event(msg, self.last_timestamp_ms)
        if self.loglevel_timeout:
            self.loglevel_timeout.update(loglevel, self.last_timestamp_ms, self._direct_write)
        self.ostream.write(clp_msg)

    # Added to logging.StreamHandler in python 3.7
    # override
    def setStream(self, stream: IO[bytes]) -> Optional[IO[bytes]]:
        if not self.stream:
            self.stream = stream
            self.init(stream)
            return None
        elif stream is self.stream:
            return None
        else:
            self.acquire()
            try:
                self.close()
                self.stream = stream
                self.init(stream)
            finally:
                self.release()
            return self.stream

    # override
    def close(self) -> None:
        if self.loglevel_timeout:
            self.loglevel_timeout.timeout()
        self.ostream.write(EOF_CHAR)
        self.ostream.close()
        self.closed = True
        super().close()


class CLPFileHandler(CLPStreamHandler):
    """
    Wrapper class that calls `open` for convenience.
    """

    def __init__(
        self,
        fpath: Path,
        mode: str = "ab",
        enable_compression: bool = True,
        timestamp_format: Optional[str] = None,
        timezone: Optional[str] = None,
        loglevel_timeout: Optional[CLPLogLevelTimeout] = None,
    ) -> None:
        self.fpath: Path = fpath
        super().__init__(
            open(fpath, mode), enable_compression, timestamp_format, timezone, loglevel_timeout
        )
