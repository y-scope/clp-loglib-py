import logging
from typing import Any, Dict

from clp_logging.utils import Timestamp

TIMESTAMP_KEY: str = "timestamp"
TIMESTAMP_UNIX_TS_MS: str = "unix_ts_ms"
TIMESTAMP_UTC_OFFSET_SEC: str = "utc_offset_sec"

LEVEL_KEY: str = "level"
LEVEL_NO_KEY: str = "no"
LEVEL_NAME_KEY: str = "name"

SOURCE_CONTEXT_KEY: str = "source_context"
SOURCE_CONTEXT_PATH_KEY: str = "path"
SOURCE_CONTEXT_LINE_KEY: str = "line"

class AutoGeneratedKeyValuePairsBuffer:
    """
    A reusable buffer for auto-generated key-value pairs.

    This buffer maintains a predefined dictionary for common metadata fields, to
    enable efficient reuse without creating new dictionaries for each log event.
    """

    def __init__(self) -> None:
        self._buf: Dict[str, Any] = {
            TIMESTAMP_KEY: {
                TIMESTAMP_UNIX_TS_MS: None,
                TIMESTAMP_UTC_OFFSET_SEC: None,
            },
            LEVEL_KEY: {
                LEVEL_NO_KEY: None,
                LEVEL_NAME_KEY: None,
            },
            SOURCE_CONTEXT_KEY: {
                SOURCE_CONTEXT_PATH_KEY: None,
                SOURCE_CONTEXT_LINE_KEY: None,
            },
        }

    def generate(self, ts: Timestamp, record: logging.LogRecord) -> Dict[str, Any]:
        """
        Generates the auto-generated key-value pairs by populating the
        underlying buffer with the given log event metadata.

        :param ts: The timestamp assigned to the log event.
        :param record: The LogRecord containing metadata for the log event.
        :return: The populated underlying buffer as the auto-generated key-value
            pairs.
        """

        self._buf[TIMESTAMP_KEY][TIMESTAMP_UNIX_TS_MS] = ts.get_unix_ts()
        self._buf[TIMESTAMP_KEY][TIMESTAMP_UTC_OFFSET_SEC] = ts.get_utc_offset()

        # NOTE: We don't add all the metadata contained in `record`. Instead, we only add the
        # following fields:
        # - Log level
        # - Source context

        self._buf[LEVEL_KEY][LEVEL_NO_KEY] = record.levelno
        self._buf[LEVEL_KEY][LEVEL_NAME_KEY] = record.levelname

        self._buf[SOURCE_CONTEXT_KEY][SOURCE_CONTEXT_PATH_KEY] = record.pathname
        self._buf[SOURCE_CONTEXT_KEY][SOURCE_CONTEXT_LINE_KEY] = record.lineno

        return self._buf
