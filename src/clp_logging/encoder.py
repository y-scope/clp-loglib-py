import json
from math import floor
import re
import time
from typing import Dict, Match, Pattern

from clp_logging.protocol import (
    BYTE_ORDER,
    DELIM_DICT,
    DELIM_INT,
    DELIM_FLOAT,
    LOGTYPE_STR_LEN_UBYTE,
    LOGTYPE_STR_LEN_USHORT,
    LOGTYPE_STR_LEN_INT,
    MAGIC_NUMBER_COMPACT_ENCODING,
    Metadata,
    METADATA_JSON_ENCODING,
    METADATA_LEN_UBYTE,
    METADATA_LEN_USHORT,
    METADATA_REFERENCE_TIMESTAMP_KEY,
    METADATA_TIMESTAMP_PATTERN_KEY,
    METADATA_TZ_ID_KEY,
    METADATA_VERSION_KEY,
    METADATA_VERSION_VALUE,
    RE_DELIM_VAR,
    RE_SUB_DELIM_VAR,
    TIMESTAMP_DELTA_BYTE,
    TIMESTAMP_DELTA_SHORT,
    TIMESTAMP_DELTA_INT,
    VAR_COMPACT_ENCODING,
    VAR_STR_LEN_UBYTE,
    VAR_STR_LEN_USHORT,
    VAR_STR_LEN_INT,
    SIZEOF_BYTE,
    SIZEOF_INT,
    SIZEOF_SHORT,
    BYTE_MAX,
    BYTE_MIN,
    UBYTE_MAX,
    SHORT_MAX,
    SHORT_MIN,
    USHORT_MAX,
    INT_MAX,
    INT_MIN,
)

# We use regex rather than manually checking each byte in a loop to simplify
# the code and because looping in the interpreter is at least 4x slower than
# using regex (as it is a c extension)
RE_TOKEN: Pattern[bytes] = re.compile(rb"[--9+A-Z\\_a-z]+")
RE_DIGIT: Pattern[bytes] = re.compile(b"[0-9]")
RE_PERIOD: Pattern[bytes] = re.compile(rb"\.")
RE_NOT_NUM: Pattern[bytes] = re.compile(b"[^0-9.-]")
RE_NOT_HEX: Pattern[bytes] = re.compile(b"[^0-9A-Fa-f]")
RE_LETTER: Pattern[bytes] = re.compile(b"[A-Za-z]")

# Eight digits + decimal point (no need to include negative sign)
# Note: each byte is a digit character
FLOAT_VAR_TOKEN_MAX_BYTES: int = 9


class CLPEncoder:
    """
    Namespace for all CLP encoding functions.
    Functions encode bytes from the log record to create a CLP message.
    """

    @staticmethod
    def emit_preamble(timestamp: int, timestamp_format: str, timezone: str) -> bytearray:
        """
        Create the encoded CLP preamble for a stream of encoded log messages.
        :param timestamp: Reference timestamp used to calculate deltas emitted
        with each message.
        :param timestamp_format: Timestamp format to be use when generating the
        logs with a reader.
        :param timezone: Timezone to be use when generating the timestamp from
        Unix epoch time.
        :raises NotImplementedError: If metadata length too large
        :return: The encoded preamble
        """
        preamble: bytearray = bytearray(MAGIC_NUMBER_COMPACT_ENCODING)
        preamble += METADATA_JSON_ENCODING
        metadata: Metadata = {
            METADATA_VERSION_KEY: METADATA_VERSION_VALUE,
            METADATA_REFERENCE_TIMESTAMP_KEY: str(timestamp),
            METADATA_TIMESTAMP_PATTERN_KEY: timestamp_format,
            METADATA_TZ_ID_KEY: timezone,
        }
        json_bytes: bytes = json.dumps(metadata).encode()
        size: int = len(json_bytes)
        if size <= UBYTE_MAX:
            preamble += METADATA_LEN_UBYTE
            preamble += size.to_bytes(SIZEOF_BYTE, BYTE_ORDER)
        elif size <= USHORT_MAX:
            preamble += METADATA_LEN_USHORT
            preamble += size.to_bytes(SIZEOF_SHORT, BYTE_ORDER)
        else:
            raise NotImplementedError("Metadata length > unsigned short currently unsupported")
        preamble += json_bytes
        return preamble

    @staticmethod
    def encode_int(token: bytes, clp_msg: bytearray) -> bool:
        token_int: int
        try:
            token_int = int(token)
        except ValueError:
            return False
        if token_int > INT_MAX:
            return False
        clp_msg += VAR_COMPACT_ENCODING
        clp_msg += token_int.to_bytes(SIZEOF_INT, BYTE_ORDER, signed=True)
        return True

    """
    Note that the custom encoding supports lossless encoding of a
    floating-point numbers meeting the conditions mentioned in the JavaDoc
    comment. This allows us to losslessly encode the majority of printable
    floating point numbers. Some numbers, like (1/3) -> "0.3333333333333333"
    cannot be stored in this format; instead, we store these types of variables
    in the dictionary.

    NOTE: Parsing logic adapted from CLP's C++ parser and optimized for
    zero-copy parsing

    Encode into 32 bits with the following format (from MSB to LSB):
    -   1 bit: Is negative
    - 25 bits: The digits of the double without the decimal, as an integer
    -  3 bits: # of decimal digits minus 1
       - This format can represent doubles with between 1 and 8 decimal digits,
         so we use 3 bits and map the range [1, 8] to [0x0, 0x7]
    -  3 bits: Offset of the decimal from the right minus 1
       - To see why the offset is taken from the right, consider
         (1) "-1234567.8", (2) "-.12345678", and (3) ".12345678"
         - For (1), the decimal point is at offset 8 from the left and offset 1
           from the right
         - For (2), the decimal point is at offset 1 from the left and offset 8
           from the right
         - For (3), the decimal point is at offset 0 from the left and offset 8
           from the right
         - So if we take the decimal offset from the left, it can range from 0
           to 8 because of the negative sign. Whereas from the right, the
           negative sign is inconsequential.
       - Thus, we use 3 bits and map the range [1, 8] to [0x0, 0x7]
    """

    @staticmethod
    def encode_float(token: bytes, clp_msg: bytearray) -> bool:
        """
        Encode `token` to float and append to `clp_msg`
        :param token: Guaranteed to contain: at least one digit (0-9) and at
        least one period (decimal point). However, could be malformed with
        multiple periods or negative signs.
        :param clp_msg: The CLP IR stream to append the encoded float to
        :return: `False` if not a valid `float` otherwise `True` on success
        """
        negative: bool = False
        if token[0:1] == b"-":
            negative = True
            token = token[1:]

        # Malformed if we find another negative sign
        if token.find(b"-") != -1:
            return False

        if len(token) > FLOAT_VAR_TOKEN_MAX_BYTES:
            return False

        integer, *fractions = token.split(b".")
        # Malformed if we find multiple decimal points / periods
        if len(fractions) != 1:
            return False
        fraction: bytes = fractions[0]
        # Malformed if fraction empty (token is probably not actually a float)
        if len(fraction) == 0:
            return False

        # Concat digits then convert
        digits_bytes: bytes = integer + fraction
        digits_int: int = int(digits_bytes)
        # Malformed if greater than 25 bits required to store
        if digits_int >= (1 << 25):
            return False

        backing_int: int = digits_int << 6
        backing_int += (len(digits_bytes) - 1) << 3
        backing_int += len(fraction) - 1
        if negative:
            backing_int |= 1 << 31

        clp_msg += VAR_COMPACT_ENCODING
        clp_msg += backing_int.to_bytes(SIZEOF_INT, BYTE_ORDER)
        return True

    @staticmethod
    def encode_dict(token: bytes, clp_msg: bytearray) -> None:
        size: int = len(token)
        if size <= UBYTE_MAX:
            clp_msg += VAR_STR_LEN_UBYTE
            clp_msg += size.to_bytes(SIZEOF_BYTE, BYTE_ORDER)
        elif size <= USHORT_MAX:
            clp_msg += VAR_STR_LEN_USHORT
            clp_msg += size.to_bytes(SIZEOF_SHORT, BYTE_ORDER)
        elif size <= INT_MAX:
            clp_msg += VAR_STR_LEN_INT
            clp_msg += size.to_bytes(SIZEOF_INT, BYTE_ORDER, signed=True)
        else:
            raise NotImplementedError("Dictvar length > signed int currently unsupported")
        clp_msg += token

    @staticmethod
    def encode_logtype(logtype: bytes, clp_msg: bytearray) -> None:
        size: int = len(logtype)
        if size <= UBYTE_MAX:
            clp_msg += LOGTYPE_STR_LEN_UBYTE
            clp_msg += size.to_bytes(SIZEOF_BYTE, BYTE_ORDER)
        elif size <= USHORT_MAX:
            clp_msg += LOGTYPE_STR_LEN_USHORT
            clp_msg += size.to_bytes(SIZEOF_SHORT, BYTE_ORDER)
        elif size <= INT_MAX:
            clp_msg += LOGTYPE_STR_LEN_INT
            clp_msg += size.to_bytes(SIZEOF_INT, BYTE_ORDER, signed=True)
        else:
            raise NotImplementedError("Logtype length > signed int currently unsupported")
        clp_msg += logtype

    @staticmethod
    def emit_token(token_m: Match[bytes], clp_msg: bytearray) -> bytes:
        """
        Encode `token_m` appending it to `clp_msg` if it is a variable.
        :return: If the token was a variable returns the delimiter to append to
        logtype, otherwise returns the static text to append to the logtype.
        """
        token: bytes = token_m.group(0)

        # Token contains decimal digit
        if RE_DIGIT.search(token):
            # Token contains byte not possible in int or float [^0-9.-]
            if RE_NOT_NUM.search(token):
                CLPEncoder.encode_dict(token, clp_msg)
                return DELIM_DICT
            # Token contains a period (decimal point)
            elif RE_PERIOD.search(token):
                if CLPEncoder.encode_float(token, clp_msg):
                    return DELIM_FLOAT
            else:
                if CLPEncoder.encode_int(token, clp_msg):
                    return DELIM_INT
            CLPEncoder.encode_dict(token, clp_msg)
            return DELIM_DICT

        # Token is possible multi-char hex number
        if len(token) > 1 and RE_NOT_HEX.search(token) is None:
            CLPEncoder.encode_dict(token, clp_msg)
            return DELIM_DICT

        # Token contains a letter and follows '='
        start: int = token_m.start()
        if start > 0 and token_m.string[start - 1 : start] == b"=" and RE_LETTER.search(token):
            CLPEncoder.encode_dict(token, clp_msg)
            return DELIM_DICT

        # Token is static text (not a variable)
        return token

    @staticmethod
    def encode_timestamp(last_timestamp_ms: int, buf: bytearray) -> int:
        """
        Encode the timestamp delta between `last_timestamp_ms` and the
        current `time()` into `buf`
        :raises NotImplementedError: If unsupported timestamp delta size
        :return: The current timestamp (from `time.time()`)
        """
        timestamp_ms: int = floor(time.time() * 1000)  # convert to ms and truncate
        delta: int = timestamp_ms - last_timestamp_ms
        if BYTE_MAX >= delta >= BYTE_MIN:
            buf += TIMESTAMP_DELTA_BYTE
            buf += delta.to_bytes(SIZEOF_BYTE, BYTE_ORDER, signed=True)
        elif SHORT_MAX >= delta >= SHORT_MIN:
            buf += TIMESTAMP_DELTA_SHORT
            buf += delta.to_bytes(SIZEOF_SHORT, BYTE_ORDER, signed=True)
        elif INT_MAX >= delta >= INT_MIN:
            buf += TIMESTAMP_DELTA_INT
            buf += delta.to_bytes(SIZEOF_INT, BYTE_ORDER, signed=True)
        else:
            raise NotImplementedError("Timestamp delta > signed int currently unsupported")
        return timestamp_ms

    @staticmethod
    def encode_message(msg: bytes) -> bytearray:
        """
        Encode the log `msg` returned from a handler's `format` call with a
        `logging.LogRecord`
        """
        # Escape dangerous bytes
        msg = RE_DELIM_VAR.sub(RE_SUB_DELIM_VAR, msg)

        clp_msg: bytearray = bytearray()
        logtype: bytearray = bytearray()

        pos: int = 0
        for token in RE_TOKEN.finditer(msg):
            start, end = token.span()
            if start > 0:
                logtype += msg[pos:start]
            logtype += CLPEncoder.emit_token(token, clp_msg)
            pos = end
        if pos < len(msg):
            logtype += msg[pos:]

        logtype += b"\n"
        CLPEncoder.encode_logtype(logtype, clp_msg)
        return clp_msg
