import logging
import os
import time
import unittest
from datetime import datetime, timedelta, tzinfo
from math import floor
from pathlib import Path
from typing import IO, List, Optional

import dateutil.parser

from clp_logging.handlers import (
    CLPBaseHandler,
    CLPFileHandler,
    CLPSockHandler,
    CLPStreamHandler,
    DEFAULT_LOG_FORMAT,
)
from clp_logging.readers import CLPFileReader

LOG_DIR: Path = Path("unittest-logs")


class DtStreamHandler(logging.StreamHandler):
    """`logging` handler using `datetime` for the timestamp rather than `time`
    (used internally by `logging`). Necessary for correct comparison with CLP
    log handlers.
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
    """Functionally abstract as we use `load_tests` to avoid adding
    `TestCLPBase` itself to the test suite. This allows us to share tests
    between different handlers.
    However, we cannot mark it as abstract as `unittest` will still `__init__`
    an instance before `load_tests` is run (and will error out if any method
    is marked abstract)
    """

    clp_handler: logging.Handler
    raw_log_path: Path
    clp_log_path: Path

    # override
    @classmethod
    def setUpClass(cls) -> None:
        if not LOG_DIR.exists():
            LOG_DIR.mkdir(parents=True, exist_ok=True)
        assert LOG_DIR.is_dir()

    # override
    def setUp(self) -> None:
        self.raw_log_path: Path = LOG_DIR / Path(f"{self.id()}.log")
        self.clp_log_path: Path = LOG_DIR / Path(f"{self.id()}.clp.zst")

    def cleanup(self) -> None:
        if self.raw_log_path.exists():
            self.raw_log_path.unlink()
        if self.clp_log_path.exists():
            self.clp_log_path.unlink()

    def read_clp(self) -> List[str]:
        with CLPFileReader(self.clp_log_path) as logf:
            return [log.log for log in logf]

    def read_raw(self) -> List[str]:
        with open(self.raw_log_path, "r") as logf:
            return logf.readlines()

    def setup_logging(self) -> None:
        self.logger: logging.Logger = logging.getLogger(self.id())
        self.logger.setLevel(logging.INFO)

        self.raw_handler: DtFileHandler = DtFileHandler(self.raw_log_path)
        self.logger.addHandler(self.clp_handler)
        self.logger.addHandler(self.raw_handler)

    def close(self) -> None:
        logging.shutdown()
        self.logger.removeHandler(self.clp_handler)
        self.logger.removeHandler(self.raw_handler)

    def compare_all_logs(self) -> None:
        self.close()
        clp_logs: List[str] = self.read_clp()
        raw_logs: List[str] = self.read_raw()
        self.compare_logs(clp_logs, raw_logs)

    def compare_logs(self, clp_logs: List[str], raw_logs: List[str]) -> None:
        for clp_log, raw_log in zip(clp_logs, raw_logs):
            # Assume logs are always formatted in ISO timestamp at beginning
            # Timestamp difference less than 2ms is close enough, but message
            # must be the same
            clp_log_split: List[str] = clp_log.split()
            raw_log_split: List[str] = raw_log.split()
            clp_time_str: str = " ".join(clp_log_split[0:2])
            raw_time_str: str = " ".join(raw_log_split[0:2])
            clp_msg: str = " ".join(clp_log_split[2:])
            raw_msg: str = " ".join(raw_log_split[2:])
            clp_timestamp: datetime = dateutil.parser.isoparse(clp_time_str)
            raw_timestamp: datetime = dateutil.parser.isoparse(raw_time_str)

            self.assertAlmostEqual(clp_timestamp, raw_timestamp, delta=timedelta(milliseconds=2))
            self.assertEqual(clp_msg, raw_msg)

    def assert_clp_logs(self, expected_logs: List[str]) -> None:
        self.close()
        clp_logs: List[str] = self.read_clp()
        for clp_log, expected_log in zip(clp_logs, expected_logs):
            clp_msg: str = " ".join(clp_log.split()[2:])
            self.assertEqual(clp_msg, expected_log)
        self.compare_logs(clp_logs[len(expected_logs):], self.read_raw())


class TestCLPInitBase(TestCLPBase):
    def test_start_time(self) -> None:
        self.clp_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        self.raw_handler.setFormatter(logging.Formatter(" [%(levelname)s] %(message)s"))
        self.logger.info("format starts with %(asctime)s")
        self.assert_clp_logs(["[WARN][clp_logging] replacing \'%(asctime)s\' with clp_logging timestamp"])

    def test_bad_time(self) -> None:
        fmt: str = "[%(levelname)s] %(asctime)s %(message)s"
        self.clp_handler.setFormatter(logging.Formatter(fmt))
        self.logger.info("format has %(asctime)s in the middle")
        self.assert_clp_logs([f"[WARN][clp_logging] replacing \'{fmt}\' with \'{DEFAULT_LOG_FORMAT}\'"])

    def test_no_time(self) -> None:
        self.clp_handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
        self.raw_handler.setFormatter(logging.Formatter(" [%(levelname)s] %(message)s"))
        self.logger.info("no asctime in format")
        self.assert_clp_logs(["[WARN][clp_logging] prepending clp_logging timestamp to formatter"])


class TestCLPHandlerBase(TestCLPBase):
    def test_static(self) -> None:
        self.logger.info("static text log one")
        self.logger.info("static text log two")
        self.compare_all_logs()

    def test_int(self) -> None:
        self.logger.info("int 1234")
        self.logger.info("-int -1234")
        self.compare_all_logs()

    def test_float(self) -> None:
        self.logger.info("float 12.34")
        self.logger.info("-float -12.34")
        self.compare_all_logs()

    def test_dict(self) -> None:
        self.logger.info("textint test1234")
        self.logger.info("texteq=var")
        self.logger.info(f">32bit int: {2**32}")
        self.compare_all_logs()

    def test_combo(self) -> None:
        self.logger.info("zxcvbn 1234 asdfgh 12.34 qwerty")
        self.logger.info("zxcvbn -1234 asdfgh -12.34 qwerty")
        self.logger.info("zxcvbn foo=bar asdfgh foobar=var321 qwerty")
        self.compare_all_logs()


class TestCLPSockHandler(TestCLPHandlerBase):
    # override
    def setUp(self) -> None:
        super().setUp()
        self.sock_path: Path = self.clp_log_path.with_suffix(".sock")
        self.cleanup()
        self.clp_handler: CLPSockHandler
        try:
            self.clp_handler = CLPSockHandler(self.clp_log_path, create_listener=True)
        except SystemExit as e:
            self.assertEqual(e.code, 0)
            # hack to exit the forked listener process without being caught and
            # reported by unittest
            os._exit(0)
        self.setup_logging()

    def close(self) -> None:
        self.clp_handler.stop_listener()
        os.waitpid(self.clp_handler.listener_pid, 0)
        super().close()

    def cleanup(self) -> None:
        super().cleanup()
        if self.sock_path.exists():
            self.sock_path.unlink()


class TestCLPStreamHandler(TestCLPHandlerBase, TestCLPInitBase):
    # override
    def setUp(self) -> None:
        super().setUp()
        self.cleanup()
        self.clp_handler: CLPStreamHandler = CLPFileHandler(self.clp_log_path)
        self.setup_logging()


if __name__ == "__main__":
    unittest.main()
