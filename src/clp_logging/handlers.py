from abc import ABCMeta, abstractmethod
import logging
import os
import time
from datetime import datetime
from math import floor
from pathlib import Path
from queue import Empty, Queue
from signal import signal, SIGINT, SIGTERM
import socket
from threading import Thread
import sys
from types import FrameType
from typing import ClassVar, IO, Optional, Tuple, TypeVar

from zstandard import ZstdCompressor, ZstdCompressionWriter

from clp_logging.encoder import CLPEncoder
from clp_logging.protocol import BYTE_ORDER, EOF_CHAR, SIZEOF_INT, UINT_MAX

# Note: no need to quote "Queue[bytes]" in python 3.9

DEFAULT_LOG_FORMAT: str = " %(levelname)s %(name)s %(message)s"
WARN_PREFIX: str = "[WARN][clp_logging]"


def _init_timeinfo(fmt: Optional[str], tz: Optional[str]) -> Tuple[str, str]:
    """
    Set default timestamp format or timezone if not specified.
    In the future sanitization of user input should also go here.
    Currently, timestamp format defaults to a format for the Java readers, due
    to compatibility issues between language time libraries.
    (`datatime.isoformat` is always used for the timestamp format in python
    readers.)
    :param fmt: Timestamp format written in preamble to be used when generating
    the logs with a reader.
    :param tz: Timezone written in preamble to be used when generating the
    timestamp from Unix epoch time.
    """
    if not fmt:
        fmt = "yyyy-MM-d H:m:s.A"
    if not tz:
        tz = datetime.now().astimezone().strftime("%z")
    return fmt, tz


class CLPBaseHandler(logging.Handler, metaclass=ABCMeta):
    def __init__(self) -> None:
        super().__init__()
        self.formatter: logging.Formatter = logging.Formatter(DEFAULT_LOG_FORMAT)

    # override
    def setFormatter(self, fmt: Optional[logging.Formatter]) -> None:
        """
        Check user `fmt` and remove any timestamp token to avoid double
        printing the timestamp.
        """
        if not fmt or not fmt._fmt:
            return

        fmt_str: str = fmt._fmt
        style: str = ""
        if "asctime" in fmt_str:
            found: Optional[str] = None
            if fmt_str.startswith("%(asctime)s "):
                found = "%(asctime)s"
                style = "%"
            elif fmt_str.startswith("{asctime} "):
                found = "{asctime}"
                style = "{"
            elif fmt_str.startswith("${asctime} "):
                found = "${asctime}"
                style = "$"

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
        self._write(f" {WARN_PREFIX} {msg}")

    @abstractmethod
    def _write(self, msg: str) -> None:
        raise NotImplementedError("_write must be implemented by derived handlers")

    @abstractmethod
    def emit(self, record: logging.LogRecord) -> None:
        raise NotImplementedError("emit must be implemented by derived handlers")


class CLPSockListener:
    """
    Server that listens to a named Unix domain socket for `CLPSockHandler`
    instances writing CLP IR. Can be started by explicitly calling
    `CLPSockListener.fork` or by instantiating a `CLPSockHandler` with
    `create_listener=True`.
    Can be stopped by either sending the process SIGINT, SIGTERM, or `EOF_CHAR`.
    """

    _signaled: ClassVar[bool] = False

    @staticmethod
    def _try_bind(sock: socket.socket, sock_path: Path) -> int:
        """
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
        log_queue: "Queue[bytes]",
    ) -> int:
        """
        Continuously reads from an individual `CLPSockHandler` and sends the
        received messages to the aggregator thread.
        :param conn: Client socket where encoded messages and their length are
        received.
        :param log_queue: Queue with `CLPSockListener._aggregator` thread to
        write encoded messages.
        """
        size_buf: bytearray = bytearray(SIZEOF_INT)
        while not CLPSockListener._signaled:
            try:
                read: int = conn.recv_into(size_buf, SIZEOF_INT)
                assert read == SIZEOF_INT
                size: int = int.from_bytes(size_buf, BYTE_ORDER)
                if size == 0:
                    log_queue.put(EOF_CHAR)
                    break
                buf: bytearray = bytearray(size)
                view: memoryview = memoryview(buf)
                i: int = 0
                while i < size:
                    read = conn.recv_into(view[i:], size)
                    if read == 0:
                        raise OSError("handler conn.recv_into returned 0 before finishing")
                    i += read
                log_queue.put(buf)
            except socket.timeout:  # TODO replaced with TimeoutError in python 3.10
                pass
            except OSError:
                conn.close()
                raise
        return 0

    @staticmethod
    def _aggregator(
        log_path: Path,
        log_queue: "Queue[bytes]",
        timestamp_format: Optional[str],
        timezone: Optional[str],
        timeout: int,
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
        returning and not closing properly on signal/EOF_CHAR.
        :return: 0 on successful exit
        """
        cctx: ZstdCompressor = ZstdCompressor()
        timestamp_format, timezone = _init_timeinfo(timestamp_format, timezone)
        last_timestamp_ms: int = floor(time.time() * 1000)  # convert to ms and truncate

        with log_path.open("ab") as log, cctx.stream_writer(log) as zstream:
            zstream.write(CLPEncoder.emit_preamble(last_timestamp_ms, timestamp_format, timezone))
            while not CLPSockListener._signaled:
                msg: bytes
                try:
                    msg = log_queue.get(timeout=timeout)
                except Empty:
                    continue
                if msg == EOF_CHAR:
                    break
                ts_buf: bytearray = bytearray()
                last_timestamp_ms = CLPEncoder.encode_timestamp(last_timestamp_ms, ts_buf)
                zstream.write(msg)
                zstream.write(ts_buf)
            zstream.write(EOF_CHAR)
            zstream.flush()
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
    ) -> int:
        """
        The `CLPSockListener` server function run in a new process.
        Writes 1 byte back to parent process for synchronization.
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
        log_queue: "Queue[bytes]" = Queue()
        ret: int = CLPSockListener._try_bind(sock, sock_path)
        sock.listen()
        os.write(parent_fd, b"\x00")
        if ret < 0:
            return ret

        signal(SIGINT, CLPSockListener._exit_handler)
        signal(SIGTERM, CLPSockListener._exit_handler)

        Thread(
            target=CLPSockListener._aggregator,
            args=(log_path, log_queue, timestamp_format, timezone, timeout),
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
    ) -> int:
        """
        Fork a process running `CLPSockListener._server` and use `os.setsid()`
        to give it another session id (and process group id). The parent will
        not return until the forked listener has either bound the socket
        or finished trying to.
        :param log_path: Path to log file and used to derive socket name.
        :param timestamp_format: Timestamp format written in preamble to be
        used when generating the logs with a reader.
        :param timezone: Timezone written in preamble to be used when
        generating the timestamp from Unix epoch time.
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
                    wfd, log_path, sock_path, timestamp_format, timezone, timeout
                )
            )
        else:
            os.read(rfd, 1)
        return pid


class CLPSockHandler(CLPBaseHandler):
    """
    Similar to `logging.Handler.SocketHandler`, but the log is written to the
    socket in CLP IR encoding rather than bytes. It is also simplified to only
    work with Unix domain sockets.
    The log is written to a socket named `log_path.with_suffix(".sock")`
    """

    def __init__(
        self,
        log_path: Path,
        create_listener: bool = False,
        timestamp_format: Optional[str] = None,
        timezone: Optional[str] = None,
        timeout: int = 2,
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
        sock_path: Path = log_path.with_suffix(".sock")
        self.closed: bool = False
        self.sock: socket.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.listener_pid: int = 0

        if self.sock.connect_ex(str(sock_path)) != 0:
            if create_listener:
                self.listener_pid = CLPSockListener.fork(
                    log_path, timestamp_format, timezone, timeout
                )

            # If we fail to connect again, the listener failed to resolve any
            # issues, so we raise an exception as there is nothing new to try
            try:
                self.sock.connect(str(sock_path))
            except OSError:
                self.sock.close()
                raise

    # override
    def _write(self, msg: str) -> None:
        try:
            if self.closed:
                raise RuntimeError("Socket already closed")
            clp_msg: bytearray = CLPEncoder.encode_message(msg.encode())
            size: int = len(clp_msg)
            if size > UINT_MAX:
                raise NotImplementedError(
                    "Encoded message longer than UINT_MAX currently unsupported"
                )
            sizeb: bytes = size.to_bytes(SIZEOF_INT, BYTE_ORDER)
            self.sock.sendall(sizeb)
            self.sock.sendall(clp_msg)
        except Exception as e:
            self.sock.close()
            raise e

    # override
    def emit(self, record: logging.LogRecord) -> None:
        msg: str = self.format(record)
        try:
            self._write(msg)
        except Exception:
            self.handleError(record)

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
        self.sock.send((0).to_bytes(SIZEOF_INT, BYTE_ORDER))
        self.close()


class CLPStreamHandler(CLPBaseHandler):
    """
    Similar to `logging.StreamHandler`, but the log is written to `stream`
    in CLP IR encoding rather than bytes or a string.
    :param stream: Output stream of bytes to write CLP encoded log messages to
    """

    def init(self, stream: IO[bytes]) -> None:
        self.cctx: ZstdCompressor = ZstdCompressor()
        self.zstream: ZstdCompressionWriter = self.cctx.stream_writer(stream)
        self.last_timestamp_ms: int = floor(time.time() * 1000)  # convert to ms and truncate
        self.zstream.write(
            CLPEncoder.emit_preamble(self.last_timestamp_ms, self.timestamp_format, self.timezone)
        )

    def __init__(
        self,
        stream: Optional[IO[bytes]],
        timestamp_format: Optional[str] = None,
        timezone: Optional[str] = None,
    ) -> None:
        super().__init__()
        self.closed: bool = False
        if stream is None:
            stream = sys.stderr.buffer
        self.stream: IO[bytes] = stream
        self.timestamp_format: str
        self.timezone: str
        self.timestamp_format, self.timezone = _init_timeinfo(timestamp_format, timezone)
        self.init(self.stream)

    # override
    def _write(self, msg: str) -> None:
        if self.closed:
            raise RuntimeError("Stream already closed")
        clp_msg: bytearray = CLPEncoder.encode_message(msg.encode())
        self.last_timestamp_ms = CLPEncoder.encode_timestamp(self.last_timestamp_ms, clp_msg)
        self.zstream.write(clp_msg)

    # override
    def emit(self, record: logging.LogRecord) -> None:
        msg: str = self.format(record)
        try:
            self._write(msg)
        except Exception:
            self.handleError(record)

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
        self.zstream.flush()
        self.zstream.write(EOF_CHAR)
        self.zstream.close()
        self.closed = True
        super().close()


class CLPFileHandler(CLPStreamHandler):
    """Wrapper class that calls `open` for convenience."""

    def __init__(
        self,
        fpath: Path,
        mode: str = "ab",
        timestamp_format: Optional[str] = None,
        timezone: Optional[str] = None,
    ) -> None:
        self.fpath: Path = fpath
        super().__init__(open(fpath, mode))
