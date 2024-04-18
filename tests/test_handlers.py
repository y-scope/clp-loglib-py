import logging
import os
import signal
import time
import unittest
from ctypes import c_double, c_int
from datetime import datetime, tzinfo
from math import floor
from multiprocessing.sharedctypes import Array, Synchronized, SynchronizedArray, Value
from pathlib import Path
from typing import cast, Dict, IO, List, Optional, Union

import dateutil.parser
from smart_open import open, register_compressor  # type: ignore
from zstandard import (
    ZstdCompressionWriter,
    ZstdCompressor,
    ZstdDecompressionReader,
    ZstdDecompressor,
)

from clp_logging.handlers import (
    CLPBaseHandler,
    CLPFileHandler,
    CLPLogLevelTimeout,
    CLPSockHandler,
    CLPStreamHandler,
    DEFAULT_LOG_FORMAT,
    WARN_PREFIX,
)
from clp_logging.protocol import Metadata
from clp_logging.readers import CLPFileReader, CLPSegmentStreaming


def _zstd_comppressions_handler(
    file_obj: IO[bytes], mode: str
) -> Union[ZstdCompressionWriter, ZstdDecompressionReader]:
    if "wb" == mode:
        cctx = ZstdCompressor()
        return cctx.stream_writer(file_obj)
    elif "rb" == mode:
        dctx = ZstdDecompressor()
        return dctx.stream_reader(file_obj)
    else:
        raise RuntimeError(f"Zstd handler: Unexpected Mode {mode}")


# Register .zst with zstandard library compressor
register_compressor(".zst", _zstd_comppressions_handler)

LOG_DIR: Path = Path("unittest-logs")
FATAL_EXIT_CODE_BASE: int = 128

ASSERT_TIMESTAMP_DELTA_S: float = 0.256
LOG_DELAY_S: float = 0.064
TIMEOUT_PADDING_S: float = 0.512


def _try_waitpid(target_pid: int) -> int:
    """
    Poll for target_pid to finish by repeatedly sleeping and checking waitpid
    with WNOHANG. If waitpid has not returned the target_pid after some delay,
    we send sigkill.

    :param target_pid: pid of target process
    :return: process exit code (0 on success) or shell fatal exit code base
        (128) + sigkill (9) (=137)
    """
    for _ in range(64):
        pid: int
        exit_code: int
        time.sleep(0.256)
        pid, exit_code = os.waitpid(target_pid, os.WNOHANG)
        if pid == target_pid:
            if 0 == exit_code:
                return 0
            else:
                return exit_code

    os.kill(target_pid, signal.SIGKILL)
    return FATAL_EXIT_CODE_BASE + signal.SIGKILL


# TODO: revisit type ignore if minimum python version increased
class DtStreamHandler(logging.StreamHandler):  # type: ignore
    """
    `logging` handler using `datetime` for the timestamp rather than `time`
    (used internally by `logging`), so we can perform correct comparison with
    CLP log handlers.
    """

    def __init__(self, stream: IO[str]) -> None:
        super().__init__(stream)
        self.timezone: Optional[tzinfo] = datetime.now().astimezone().tzinfo
        self.formatter: logging.Formatter = logging.Formatter(DEFAULT_LOG_FORMAT)

    # override
    def emit(self, record: logging.LogRecord) -> None:
        try:
            clp_time: float = floor(time.time() * 1000) / 1000
            dt: datetime = datetime.fromtimestamp(clp_time, self.timezone)
            msg: str = self.format(record)
            self.stream.write(dt.isoformat(sep=" ", timespec="milliseconds") + msg + "\n")
        except Exception:
            self.handleError(record)

    # override
    def close(self) -> None:
        self.stream.close()


class DtFileHandler(DtStreamHandler):
    def __init__(self, fpath: Path) -> None:
        self.path: Path = fpath
        super().__init__(open(fpath, "a"))


class TestCLPBase(unittest.TestCase):
    """
    Functionally abstract base class for testing handlers, etc.

    Functionally abstract as we use `load_tests` to avoid adding `TestCLPBase`
    itself to the test suite. This allows us to share tests between different
    handlers. However, we cannot mark it as abstract as `unittest` will still
    `__init__` an instance before `load_tests` is run (and will error out if any
    method is marked abstract).

    Non-base classes have shortened names to avoid errors with socket name
    length.
    """

    clp_handler: CLPBaseHandler
    raw_log_path: Path
    clp_log_path: Path
    enable_compression: bool

    # override
    @classmethod
    def setUpClass(cls) -> None:
        if not LOG_DIR.exists():
            LOG_DIR.mkdir(parents=True, exist_ok=True)
        assert LOG_DIR.is_dir()

    # override
    def setUp(self) -> None:
        self.raw_log_path: Path = LOG_DIR / Path(f"{self.id()}.log")
        self.clp_log_path: Path = LOG_DIR / Path(f"{self.id()}.clp")
        if self.raw_log_path.exists():
            self.raw_log_path.unlink()
        if self.clp_log_path.exists():
            self.clp_log_path.unlink()

    def read_clp(self) -> List[str]:
        with CLPFileReader(self.clp_log_path, enable_compression=self.enable_compression) as logf:
            return [log.formatted_msg for log in logf]

    def read_raw(self) -> List[str]:
        with open(self.raw_log_path, "r") as logf:
            logs: List[str] = logf.readlines()
            return logs

    def setup_logging(self) -> None:
        self.logger: logging.Logger = logging.getLogger(self.id())
        self.logger.setLevel(logging.DEBUG)

        self.raw_handler: DtFileHandler = DtFileHandler(self.raw_log_path)
        self.logger.addHandler(self.clp_handler)
        self.logger.addHandler(self.raw_handler)

    def close(self) -> None:
        logging.shutdown()
        self.logger.removeHandler(self.clp_handler)
        self.logger.removeHandler(self.raw_handler)

    def close_and_compare_logs(self, test_time: bool = True, size_msg: bool = False) -> None:
        self.close()
        clp_logs: List[str] = self.read_clp()
        raw_logs: List[str] = self.read_raw()
        self.compare_logs(clp_logs, raw_logs, test_time, size_msg)

    def compare_logs(
        self,
        clp_logs: List[str],
        raw_logs: List[str],
        test_time: bool = True,
        size_msg: bool = False,
    ) -> None:
        self.assertEqual(len(clp_logs), len(raw_logs))
        for clp_log, raw_log in zip(clp_logs, raw_logs):
            # Assume logs are always formatted in ISO timestamp at beginning
            clp_log_split: List[str] = clp_log.split()
            raw_log_split: List[str] = raw_log.split()

            # Timestamp difference less than 8ms is close enough, but message
            # must be the same
            if test_time:
                clp_time_str: str = " ".join(clp_log_split[0:2])
                raw_time_str: str = " ".join(raw_log_split[0:2])
                self.assertAlmostEqual(
                    dateutil.parser.isoparse(clp_time_str).timestamp(),
                    dateutil.parser.isoparse(raw_time_str).timestamp(),
                    delta=ASSERT_TIMESTAMP_DELTA_S,
                )

            clp_msg: str = " ".join(clp_log_split[2:])
            raw_msg: str = " ".join(raw_log_split[2:])
            msg: Optional[str] = None
            if size_msg:
                msg = f"len(clp_msg): {len(clp_msg)}, len(raw_msg): {len(raw_msg)}"
            self.assertEqual(clp_msg, raw_msg, msg)

    def assert_clp_logs(self, expected_logs: List[str]) -> None:
        self.close()
        clp_logs: List[str] = self.read_clp()
        for clp_log, expected_log in zip(clp_logs, expected_logs):
            # Removing timestamp from beginning of log, that we assume is
            # always ISO formatted
            clp_msg: str = " ".join(clp_log.split()[2:])
            self.assertEqual(clp_msg, expected_log)
        self.compare_logs(clp_logs[len(expected_logs) :], self.read_raw())


class TestCLPHandlerBase(TestCLPBase):
    """
    A functionally abstract class, that can be inherited by a class which
    implements the `setUp` and `close` logic required to create a concrete
    CLPHandler.

    Provides general tests where logs are generated by a normal handler and a
    CLP handler which encodes. The CLP logs are then decoded and compared with
    the ones from the raw handler.
    """

    # note we need to remove leading space from WARN_PREFIX that exists for
    # formatting in order for the asserts to pass
    def test_time_format_at_start(self) -> None:
        self.clp_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        self.raw_handler.setFormatter(logging.Formatter(" [%(levelname)s] %(message)s"))
        self.logger.info("format starts with %(asctime)s")
        self.assert_clp_logs(
            [f"{WARN_PREFIX.lstrip()} replacing '%(asctime)s' with clp_logging timestamp format"]
        )

    def test_time_format_in_middle(self) -> None:
        fmt: str = "[%(levelname)s] %(asctime)s %(message)s"
        self.clp_handler.setFormatter(logging.Formatter(fmt))
        self.logger.info("format has %(asctime)s in the middle")
        self.assert_clp_logs(
            [f"{WARN_PREFIX.lstrip()} replacing '{fmt}' with '{DEFAULT_LOG_FORMAT}'"]
        )

    def test_time_format_missing(self) -> None:
        self.clp_handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
        self.raw_handler.setFormatter(logging.Formatter(" [%(levelname)s] %(message)s"))
        self.logger.info("no asctime in format")
        self.assert_clp_logs(
            [f"{WARN_PREFIX.lstrip()} prepending clp_logging timestamp to formatter"]
        )

    def test_static(self) -> None:
        self.logger.info("static text log one")
        self.logger.info("static text log two")
        self.close_and_compare_logs()

    def test_int(self) -> None:
        self.logger.info("int 1234")
        self.logger.info("-int -1234")
        self.close_and_compare_logs()

    def test_float(self) -> None:
        self.logger.info("float 12.34")
        self.logger.info("-float -12.34")
        self.close_and_compare_logs()

    def test_dict(self) -> None:
        self.logger.info("textint test1234")
        self.logger.info("texteq=var")
        self.logger.info(f">32bit int: {2**32}")
        self.close_and_compare_logs()

    def test_combo(self) -> None:
        self.logger.info("zxcvbn 1234 asdfgh 12.34 qwerty")
        self.logger.info("zxcvbn -1234 asdfgh -12.34 qwerty")
        self.logger.info("zxcvbn foo=bar asdfgh foobar=var321 qwerty")
        self.close_and_compare_logs()

    def test_long_log(self) -> None:
        long_even_log: str = "x" * (8 * 1024 * 1024)  # 8mb
        long_odd_log: str = "x" * (8 * 1024 * 1024 - 1)
        self.logger.info(long_even_log)
        self.logger.info(long_odd_log)
        self.close_and_compare_logs(test_time=False, size_msg=True)


class TestCLPLogLevelTimeoutBase(TestCLPBase):
    """
    A functionally abstract class, that can be inherited by a class which
    implements the `setUp` or `_setup_handler` and `close` logic required to
    create a concrete CLPHandler. `_setup_handler` allows for test setup based
    on information about the particular test.

    Provides tests meant to trigger and observe the `CLPLogLevelTimeout`
    feature. Notably, `_test_timeout` provides a method to quickly create
    different timeout scenarios.
    """

    loglevel_timeout: CLPLogLevelTimeout

    def _setup_handler(self) -> None:
        raise NotImplementedError("_setup_handler must be implemented by derived testers")

    # override
    def close(self) -> None:
        if self.loglevel_timeout.hard_timeout_thread:
            self.loglevel_timeout.hard_timeout_thread.cancel()
        if self.loglevel_timeout.soft_timeout_thread:
            self.loglevel_timeout.soft_timeout_thread.cancel()
        super().close()

    def test_bad_timeout_dicts(self) -> None:
        self.loglevel_timeout = CLPLogLevelTimeout(lambda: None, {}, {})
        self._setup_handler()

        self.logger.debug("debug log0")
        self.assert_clp_logs(
            [
                (
                    f"{WARN_PREFIX.lstrip()} log level {logging.DEBUG} not in"
                    " self.hard_timeout_deltas; defaulting to _HARD_TIMEOUT_DELTAS[logging.INFO]."
                ),
                (
                    f"{WARN_PREFIX.lstrip()} log level {logging.DEBUG} not in"
                    " self.soft_timeout_deltas; defaulting to _SOFT_TIMEOUT_DELTAS[logging.INFO]."
                ),
            ]
        )

    def _test_timeout(
        self,
        loglevels: List[int],
        log_delay: float,
        hard_deltas: Dict[int, int],
        soft_deltas: Dict[int, int],
        expected_timeout_count: int,
        expected_timeout_deltas: List[float],
    ) -> None:
        """
        This helper function uses `multiprocessing.RawArray` and
        `multiprocessing.RawValue` to allow data from timeouts in the separate
        CLPSockListener process to be seen. The last expected timeout occurs on
        closing the handler.

        :param loglevels: generate one log for each entry at given log level
        :param log_delay: (fraction of) seconds to sleep between `logger.log`
            calls
        :param hard_deltas: deltas in ms to initialize `CLPLogLevelTimeout`
        :param soft_deltas: deltas in ms to initialize `CLPLogLevelTimeout`
        :param expected_timeout_count: expected number of timeouts to observe
        :param expected_timeout_deltas: expected elapsed time from start of
        logging to timeout `i`
        """

        # typing for multiprocess.Synchronized* has open issues
        # https://github.com/python/typeshed/issues/8799
        # TODO: when the issue is closed we should update the typing here
        timeout_ts: SynchronizedArray[c_double] = Array(c_double, [0.0] * expected_timeout_count)
        timeout_count: Synchronized[int] = cast("Synchronized[int]", Value(c_int, 0))

        def timeout_fn() -> None:
            nonlocal timeout_ts
            nonlocal timeout_count
            timeout_ts[timeout_count.value] = c_double(time.time())
            timeout_count.value += 1

        self.loglevel_timeout = CLPLogLevelTimeout(timeout_fn, hard_deltas, soft_deltas)
        self._setup_handler()

        start_ts: float = time.time()
        for i, loglevel in enumerate(loglevels):
            self.logger.log(loglevel, f"log{i} with loglevel={loglevel}")
            time.sleep(log_delay)

        # We want sleep long enough so that the final expected timeout can
        # occur, but also ensure time.sleep recieves a non-negative number.
        time_to_last_timeout: float = max(0, (start_ts + expected_timeout_deltas[-1]) - time.time())
        time.sleep(time_to_last_timeout)

        self.logger.log(logging.INFO, "ensure close flushes correctly")
        self.close_and_compare_logs()
        self.assertEqual(timeout_count.value, expected_timeout_count)
        for i in range(expected_timeout_count):
            self.assertAlmostEqual(
                timeout_ts[i],  # type: ignore
                start_ts + expected_timeout_deltas[i],
                delta=ASSERT_TIMESTAMP_DELTA_S,
            )

    def test_pushback_soft_timeout(self) -> None:
        log_delay: float = LOG_DELAY_S
        soft_delta_s: float = log_delay * 2
        soft_delta_ms: int = int(soft_delta_s * 1000)
        self._test_timeout(
            loglevels=[logging.INFO, logging.INFO, logging.INFO],
            log_delay=log_delay,
            hard_deltas={logging.INFO: 30 * 60 * 1000},
            soft_deltas={logging.INFO: soft_delta_ms},
            # log_delay < soft delta, so timeout push back should occur
            # timeout = final log occurrence + soft delta
            expected_timeout_count=2,
            expected_timeout_deltas=[
                (2 * log_delay) + soft_delta_s,
                (2 * log_delay) + soft_delta_s + TIMEOUT_PADDING_S,
            ],
        )

    def test_multiple_soft_timeout(self) -> None:
        log_delay: float = LOG_DELAY_S * 2
        soft_delta_s: float = LOG_DELAY_S
        soft_delta_ms: int = int(soft_delta_s * 1000)
        self._test_timeout(
            loglevels=[logging.INFO, logging.INFO, logging.INFO],
            log_delay=log_delay,
            hard_deltas={logging.INFO: 30 * 60 * 1000},
            soft_deltas={logging.INFO: soft_delta_ms},
            # log_delay > soft delta, so every log will timeout
            expected_timeout_count=4,
            expected_timeout_deltas=[
                soft_delta_s,
                log_delay + soft_delta_s,
                (2 * log_delay) + soft_delta_s,
                (2 * log_delay) + soft_delta_s + TIMEOUT_PADDING_S,
            ],
        )

    def test_hard_timeout(self) -> None:
        log_delay: float = LOG_DELAY_S
        hard_delta_s: float = LOG_DELAY_S * 4
        hard_delta_ms: int = int(hard_delta_s * 1000)
        self._test_timeout(
            loglevels=[logging.INFO, logging.INFO, logging.INFO],
            log_delay=log_delay,
            hard_deltas={logging.INFO: hard_delta_ms},
            soft_deltas={logging.INFO: 3 * 60 * 1000},
            # hard timeout triggered by first log will occur shortly after the
            # 3rd log, no pushback occurs
            expected_timeout_count=2,
            expected_timeout_deltas=[
                hard_delta_s,
                hard_delta_s + TIMEOUT_PADDING_S,
            ],
        )

    def test_end_timeout(self) -> None:
        self._test_timeout(
            loglevels=[logging.INFO, logging.INFO, logging.INFO],
            log_delay=LOG_DELAY_S,
            hard_deltas={logging.INFO: 30 * 60 * 1000},
            soft_deltas={logging.INFO: 3 * 60 * 1000},
            # no deltas occur
            # timeout = when close is called roughly after last log
            expected_timeout_count=1,
            expected_timeout_deltas=[
                (3 * LOG_DELAY_S) + TIMEOUT_PADDING_S,
            ],
        )


class TestCLPSockHandlerBase(TestCLPHandlerBase):
    # override
    def setUp(self) -> None:
        super().setUp()
        self.sock_path: Path = self.clp_log_path.with_suffix(".sock")
        if self.sock_path.exists():
            self.sock_path.unlink()

        self.clp_handler: CLPSockHandler
        try:
            self.clp_handler = CLPSockHandler(
                self.clp_log_path, create_listener=True, enable_compression=self.enable_compression
            )
        except SystemExit as e:
            self.assertEqual(e.code, 0)
            # hack to exit the forked listener process without being caught and
            # reported by unittest
            os._exit(0)
        self.setup_logging()

    # override
    def close(self) -> None:
        self.clp_handler.stop_listener()
        self.assertEqual(0, _try_waitpid(self.clp_handler.listener_pid))
        super().close()


class TestCLPSock_ZSTD(TestCLPSockHandlerBase):
    # override
    def setUp(self) -> None:
        self.enable_compression = True
        super().setUp()


class TestCLPSock_RAW(TestCLPSockHandlerBase):
    # override
    def setUp(self) -> None:
        self.enable_compression = False
        super().setUp()


class TestCLPSockHandlerLogLevelTimeoutBase(TestCLPLogLevelTimeoutBase):
    # override
    def setUp(self) -> None:
        TestCLPLogLevelTimeoutBase.setUp(self)
        self.sock_path: Path = self.clp_log_path.with_suffix(".sock")
        if self.sock_path.exists():
            self.sock_path.unlink()

    # override
    def _setup_handler(self) -> None:
        self.clp_handler: CLPSockHandler
        try:
            self.clp_handler = CLPSockHandler(
                self.clp_log_path,
                create_listener=True,
                loglevel_timeout=self.loglevel_timeout,
                enable_compression=self.enable_compression,
            )
        except SystemExit as e:
            self.assertEqual(e.code, 0)
            # hack to exit the forked listener process without being caught and
            # reported by unittest
            os._exit(0)
        self.setup_logging()

    # override
    def close(self) -> None:
        self.clp_handler.stop_listener()
        self.assertEqual(0, _try_waitpid(self.clp_handler.listener_pid))
        super().close()


@unittest.skipIf(
    "macOS" == os.getenv("RUNNER_OS"),
    "Github macos runner tends to fail LLT tests with timing issues.",
)
class TestCLPSock_LLT_ZSTD(TestCLPSockHandlerLogLevelTimeoutBase):
    # override
    def setUp(self) -> None:
        self.enable_compression = True
        super().setUp()


@unittest.skipIf(
    "macOS" == os.getenv("RUNNER_OS"),
    "Github macos runner tends to fail LLT tests with timing issues.",
)
class TestCLPSock_LLT_RAW(TestCLPSockHandlerLogLevelTimeoutBase):
    # override
    def setUp(self) -> None:
        self.enable_compression = False
        super().setUp()


class TestCLPStream_ZSTD(TestCLPHandlerBase):
    # override
    def setUp(self) -> None:
        self.enable_compression = True
        super().setUp()
        self.clp_handler: CLPStreamHandler = CLPFileHandler(
            self.clp_log_path, enable_compression=True
        )
        self.setup_logging()


class TestCLPStream_RAW(TestCLPHandlerBase):
    # override
    def setUp(self) -> None:
        self.enable_compression = False
        super().setUp()
        self.clp_handler: CLPStreamHandler = CLPFileHandler(
            self.clp_log_path, enable_compression=False
        )
        self.setup_logging()


@unittest.skipIf(
    "macOS" == os.getenv("RUNNER_OS"),
    "Github macos runner tends to fail LLT tests with timing issues.",
)
class TestCLPStream_LLT_ZSTD(TestCLPLogLevelTimeoutBase):
    # override
    def _setup_handler(self) -> None:
        self.enable_compression = True
        self.clp_handler = CLPFileHandler(
            self.clp_log_path, loglevel_timeout=self.loglevel_timeout, enable_compression=True
        )
        self.setup_logging()


@unittest.skipIf(
    "macOS" == os.getenv("RUNNER_OS"),
    "Github macos runner tends to fail LLT tests with timing issues.",
)
class TestCLPStream_LLT_RAW(TestCLPLogLevelTimeoutBase):
    # override
    def _setup_handler(self) -> None:
        self.enable_compression = False
        self.clp_handler = CLPFileHandler(
            self.clp_log_path, loglevel_timeout=self.loglevel_timeout, enable_compression=False
        )
        self.setup_logging()


class TestCLPSegmentStreamingBase(unittest.TestCase):
    """
    Functionally abstract base class for testing segment streaming.

    Similar to `TestCLPBase`. Functionally abstract as we use `load_tests` to
    avoid adding `TestCLPSegmentStreamingBase` itself to the test suite. This
    allows us to share tests between different settings when test against IR
    segment streaming.
    """

    clp_handler: CLPBaseHandler
    clp_log_path: Path
    segment_path_list: List[Path]
    segment_idx: int
    logger: logging.Logger
    # Configurable:
    enable_compression: bool
    segment_size: int

    # override
    @classmethod
    def setUpClass(cls) -> None:
        if not LOG_DIR.exists():
            LOG_DIR.mkdir(parents=True, exist_ok=True)
        assert LOG_DIR.is_dir()

    # override
    def setUp(self) -> None:
        if self.enable_compression:
            self.clp_log_path: Path = LOG_DIR / Path(f"{self.id()}.clp.zst")
        else:
            self.clp_log_path: Path = LOG_DIR / Path(f"{self.id()}.clp")
        if self.clp_log_path.exists():
            self.clp_log_path.unlink()
        self.segment_path_list: List[Path] = []
        self.segment_idx = 0

    def generate_segments(self) -> None:
        meta: Optional[Metadata] = None
        offset: int = 0
        while True:
            segment_path: Path
            if self.enable_compression:
                segment_path = LOG_DIR / Path(f"{self.id()}_seg_{self.segment_idx}.clp.zst")
            else:
                segment_path = LOG_DIR / Path(f"{self.id()}_seg_{self.segment_idx}.clp")
            if segment_path.exists():
                segment_path.unlink()
            bytes_read: int
            with open(self.clp_log_path, "rb") as fin, open(segment_path, "wb") as fout:
                bytes_read, meta = CLPSegmentStreaming.read(
                    fin, fout, offset=offset, max_bytes_to_write=self.segment_size, metadata=meta
                )
                offset += bytes_read
                self.segment_idx += 1
            self.segment_path_list.append(segment_path)
            if meta is None or bytes_read == 0:
                break

    def close(self) -> None:
        logging.shutdown()
        self.logger.removeHandler(self.clp_handler)

    def read_clp(self) -> List[str]:
        with CLPFileReader(self.clp_log_path, enable_compression=self.enable_compression) as logf:
            return [log.formatted_msg for log in logf]

    def read_segments(self) -> List[str]:
        logs: List[str] = []
        for segment_path in self.segment_path_list:
            with CLPFileReader(segment_path, enable_compression=self.enable_compression) as logf:
                logs.extend([log.formatted_msg for log in logf])
        return logs

    def close_and_compare_logs(self) -> None:
        self.close()
        self.generate_segments()
        clp_logs: List[str] = self.read_clp()
        segment_logs: List[str] = self.read_segments()
        self.compare_logs(clp_logs, segment_logs)

    def compare_logs(self, clp_logs: List[str], segment_logs: List[str]) -> None:
        self.assertEqual(len(clp_logs), len(segment_logs))
        for clp_log, segment_log in zip(clp_logs, segment_logs):
            self.assertEqual(clp_log, segment_log)

    def setup_logging(self) -> None:
        self.clp_handler: CLPStreamHandler = CLPFileHandler(
            self.clp_log_path, enable_compression=self.enable_compression
        )
        self.logger: logging.Logger = logging.getLogger(self.id())
        self.logger.setLevel(logging.DEBUG)
        self.clp_handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
        self.logger.addHandler(self.clp_handler)

    def test_log(self) -> None:
        for i in range(100):
            self.logger.info(f"Log message #{i}")
            # Static log
            self.logger.info("static text log one")
            self.logger.info("static text log two")
            # Int
            self.logger.info("int 3190")
            self.logger.info("-int -3190")
            # Float
            self.logger.info("float 31.90")
            self.logger.info("-float -31.90")
            # Dict
            self.logger.info("textint test1234")
            self.logger.info("texteq=var")
            self.logger.info(f">32bit int: {2**32}")
            # Combo
            self.logger.info("zxcvbn 1234 asdfgh 12.34 qwerty")
            self.logger.info("zxcvbn -1234 asdfgh -12.34 qwerty")
            self.logger.info("zxcvbn foo=bar asdfgh foobar=var321 qwerty")
            # Level
            self.logger.debug("zxcvbn 1234 asdfgh 12.34 qwerty")
            self.logger.debug("zxcvbn -1234 asdfgh -12.34 qwerty")
            self.logger.debug("zxcvbn foo=bar asdfgh foobar=var321 qwerty")
        self.close_and_compare_logs()


class TestCLPSegmentStreaming_ZSTD(TestCLPSegmentStreamingBase):
    # override
    def setUp(self) -> None:
        self.enable_compression = True
        self.segment_size = 4096
        super().setUp()
        self.setup_logging()


class TestCLPSegmentStreaming_RAW(TestCLPSegmentStreamingBase):
    # override
    def setUp(self) -> None:
        self.enable_compression = False
        self.segment_size = 4096
        super().setUp()
        self.setup_logging()


if __name__ == "__main__":
    unittest.main()
