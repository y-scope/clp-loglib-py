import logging
import os
import time
from datetime import datetime
from math import floor
from pathlib import Path
from signal import signal, SIGINT, SIGTERM
from socket import socket, AF_UNIX, SOCK_DGRAM
import sys
from types import FrameType
from typing import ClassVar, IO, Optional, Tuple

from zstandard import ZstdCompressor, ZstdCompressionWriter

from clp_logging.encoder import CLPEncoder
from clp_logging.protocol import EOF_CHAR


def _init_timeinfo(fmt: Optional[str], tz: Optional[str]) -> Tuple[str, str]:
    """
    Set default timestamp format or timezone if not specified.
    In the future sanitization of user input should also go here.
    Currently, timestamp format defaults to a format for the Java readers, due
    to compatibility issues between language time libraries.
    (`datatime.isoformat` is always used for the timestamp format in python
    readers.)
    :param fmt: Timestamp format written in preamble to be use when generating
    the logs with a reader.
    :param tz: Timezone written in preamble to be use when generating the
    timestamp from Unix epoch time.
    """
    if not fmt:
        fmt = "yyyy-MM-d H:m:s.A"
    if not tz:
        tz = datetime.now().astimezone().strftime("%z")
    return fmt, tz


def _set_formatter(handler: logging.Handler, formatter: Optional[logging.Formatter]) -> None:
    """
    Check user formatter and remove any timestamp token to avoid double
    printing the timestamp.
    """
    if formatter and formatter._fmt and formatter._style:
        formatter._fmt = formatter._fmt.replace("%(asctime)s", "")
        formatter._style._fmt = formatter._fmt
    handler.formatter = formatter


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
    def _try_bind(sock: socket, sock_path: Path) -> int:
        """
        Bind will fail if the socket file exists due to:
            a. Another listener currently exists and is running
                -> try to connect to it
            b. A listener existed in the past, but is now gone
                -> recovery unsupported
        :return: >= 0 on success or < 0 on failure. 0: bind succeeds, 1:
        connect succeeds, -1: nothing worked
        """
        try:
            sock.bind(str(sock_path))
            return 0
        except OSError:
            pass

        if sock.connect_ex(str(sock_path)) == 0:
            sock.close()
            return 1
        else:
            return -1

    @staticmethod
    def _exit_handler(signum: int, frame: Optional[FrameType]) -> None:
        _signaled = True

    @staticmethod
    def _run(
        parent_fd: int,
        log_path: Path,
        sock_path: Path,
        timestamp_format: Optional[str],
        timezone: Optional[str],
    ) -> int:
        """
        The `CLPSockListener` server function run in a new process.
        Writes 1 byte back to parent process for synchronization.
        :return: 0 on successful exit, -1 if `CLPSockListener._try_bind` fails
        """
        sock = socket(AF_UNIX, SOCK_DGRAM)
        ret: int = CLPSockListener._try_bind(sock, sock_path)
        os.write(parent_fd, b"\x00")
        if ret < 0:
            return ret

        signal(SIGINT, CLPSockListener._exit_handler)
        signal(SIGTERM, CLPSockListener._exit_handler)
        cctx: ZstdCompressor = ZstdCompressor()
        timestamp_format, timezone = _init_timeinfo(timestamp_format, timezone)
        last_timestamp_ms: int = floor(time.time() * 1000)  # convert to ms and truncate

        with log_path.open("ab") as log, cctx.stream_writer(log) as zstream:
            zstream.write(CLPEncoder.emit_preamble(last_timestamp_ms, timestamp_format, timezone))
            while not CLPSockListener._signaled:
                buf: bytes = sock.recv(4096)
                if buf == EOF_CHAR:
                    break
                ts_buf: bytearray = bytearray()
                last_timestamp_ms = CLPEncoder.encode_timestamp(last_timestamp_ms, ts_buf)
                zstream.write(buf)
                zstream.write(ts_buf)
            zstream.write(EOF_CHAR)
            zstream.flush()
        sock.close()
        sock_path.unlink()
        return 0

    @staticmethod
    def fork(
        log_path: Path,
        timestamp_format: Optional[str],
        timezone: Optional[str],
    ) -> int:
        """
        Fork a process running `CLPSockListener._run` and use `os.setsid()`
        to give it another session id (and process group id). The parent will
        not return until the forked listener has either bound the socket
        or finished trying to.
        :param log_path: Path to log file and used to derive socket name.
        :param timestamp_format: Timestamp format written in preamble to be use
        when generating the logs with a reader.
        :param timezone: Timezone written in preamble to be use when generating
        the timestamp from Unix epoch time.
        :return: child pid
        """
        sock_path: Path = log_path.with_suffix(".sock")
        rfd: int
        wfd: int
        rfd, wfd = os.pipe()
        pid: int = os.fork()
        if pid == 0:
            os.setsid()
            sys.exit(CLPSockListener._run(wfd, log_path, sock_path, timestamp_format, timezone))
        else:
            os.read(rfd, 1)
        return pid


class CLPSockHandler(logging.Handler):
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
        timeout: int = 1,
    ) -> None:
        """
        Constructor method that optionally spawns a `CLPSockListener`.
        :param log_path: Path to log file written by `CLPSockListener` used to
        derive socket name.
        :param create_listener: If true and the handler could not connect to an
        existing listener, CLPSockListener.fork is used to try and spawn one. This
        is safe to be used by concurrent `CLPSockHandler` instances.
        :param timestamp_format: Timestamp format written in preamble to be use when generating
        the logs with a reader. (Only used when creating a listener.)
        :param timezone: Timezone written in preamble to be use when generating the
        timestamp from Unix epoch time. (Only used when creating a listener.)
        :param timeout: `socket.socket` timeout (defaults to 1)
        """
        super().__init__()
        sock_path: Path = log_path.with_suffix(".sock")
        self.closed: bool = False
        self.sock: socket = socket(AF_UNIX, SOCK_DGRAM)
        self.sock.settimeout(timeout)
        self.listener_pid: int = 0

        if self.sock.connect_ex(str(sock_path)) != 0:
            if create_listener:
                self.listener_pid = CLPSockListener.fork(log_path, timestamp_format, timezone)

            # If we fail to connect again, the listener failed to resolve any
            # issues, so we raise an exception as there is nothing new to try
            try:
                self.sock.connect(str(sock_path))
            except OSError:
                self.sock.close()
                raise

    # override
    def setFormatter(self, fmt: Optional[logging.Formatter]) -> None:
        _set_formatter(self, fmt)

    # override
    def emit(self, record: logging.LogRecord) -> None:
        try:
            if self.closed:
                raise RuntimeError("Socket already closed")
            msg = self.format(record)
            self.sock.send(CLPEncoder.encode_message(msg.encode()))
        except Exception:
            self.sock.close()
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
        self.sock.send(EOF_CHAR)
        self.close()


class CLPStreamHandler(logging.Handler):
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
    def setFormatter(self, fmt: Optional[logging.Formatter]) -> None:
        _set_formatter(self, fmt)

    # override
    def emit(self, record: logging.LogRecord) -> None:
        try:
            if self.closed:
                raise RuntimeError("Stream already closed")
            msg: str = self.format(record)
            clp_msg: bytearray = CLPEncoder.encode_message(msg.encode())
            self.last_timestamp_ms = CLPEncoder.encode_timestamp(self.last_timestamp_ms, clp_msg)
            self.zstream.write(clp_msg)
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
