from __future__ import annotations

import time
from math import floor


class Timestamp:
    """
    This class represents a Unix timestamp along with the timezone offset from
    UTC.
    """

    @staticmethod
    def now() -> Timestamp:
        """
        :return: A `Timestamp` instance representing the current time.
        """
        ts: float = time.time()
        return Timestamp(
            unix_ts=floor(ts * 1000),
            utc_offset=time.localtime(ts).tm_gmtoff,
        )

    def __init__(self, unix_ts: int, utc_offset: int):
        """
        Initializes a `Timestamp` instance with the current time.

        :param unix_ts: Unix timestamp in milliseconds.
        :param utc_offset: The number of seconds the timezone is ahead of
            (positive) or behind (negative) UTC.
        """
        self._utc_offset: int = utc_offset
        self._unix_ts: int = unix_ts

    def get_unix_ts(self) -> int:
        """
        :return: The Unix timestamp (milliseconds since the Unix epoch).
        """
        return self._unix_ts

    def get_utc_offset(self) -> int:
        """
        :return: The number of seconds the timezone is ahead of (positive) or behind (negative) UTC.
        """
        return self._utc_offset
