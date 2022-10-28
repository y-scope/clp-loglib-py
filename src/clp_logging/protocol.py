import re
from typing import Dict, Pattern, Tuple
from typing_extensions import Final, Literal

# Type sizing to be portable
SIZEOF_INT: Final[int] = 4
SIZEOF_SHORT: Final[int] = 2
SIZEOF_BYTE: Final[int] = 1
INT_MAX: Final[int] = (1 << 63) - 1
INT_MIN: Final[int] = ~INT_MAX
USHORT_MAX: Final[int] = (1 << 16) - 1
SHORT_MAX: Final[int] = (1 << 15) - 1
SHORT_MIN: Final[int] = SHORT_MAX
UBYTE_MAX: Final[int] = (1 << 8) - 1
BYTE_MAX: Final[int] = (1 << 7) - 1
BYTE_MIN: Final[int] = ~BYTE_MAX

# Use big to match Java DataOutputStream
BYTE_ORDER: Final[Literal["little", "big"]] = "big"

Metadata = Dict[str, str]
# Magic values are derived from Zstandard's
# https://datatracker.ietf.org/doc/html/rfc8478">magic value 0xFD2FB528
MAGIC_NUMBER_COMPACT_ENCODING: bytes = b"\xfd\x2f\xb5\x29"
METADATA_VERSION_KEY: str = "VERSION"
METADATA_VERSION_VALUE: str = "v0.0.0"
METADATA_REFERENCE_TIMESTAMP_KEY: str = "REFERENCE_TIMESTAMP"
METADATA_TIMESTAMP_PATTERN_KEY: str = "TIMESTAMP_PATTERN"
METADATA_TZ_ID_KEY: str = "TZ_ID"
METADATA_JSON_ENCODING: bytes = b"\x01"
METADATA_LEN_UBYTE: bytes = b"\x11"
METADATA_LEN_USHORT: bytes = b"\x12"
METADATA_LEN_INT: bytes = b"\x13"

DELIM_INT: bytes = b"\x11"
DELIM_DICT: bytes = b"\x12"
DELIM_FLOAT: bytes = b"\x13"
EOF_CHAR: bytes = b"\x00"
ID_EOF: int = EOF_CHAR[0]

# We need to escape variable delimiters and the escape character itself
ESCAPE_CHAR: bytes = b"\\"
# Unfortunately cannot use format strings in the following variables as the "r"
# for raw/literal string applies to the format string itself and not the delim
# variable we are substituting in
# Thus these need to be updated manually if the escape character ever changes
RE_UNESCAPE: Pattern[bytes] = re.compile(rb"\\([\x11\x12\x13\\])")
RE_SUB_UNESCAPE: bytes = rb"\1"
RE_DELIM_VAR: Pattern[bytes] = re.compile(rb"([\x11\x12\x13\\])")
RE_SUB_DELIM_VAR: bytes = rb"\\\1"

# 0x10-0x1f reserved for variable-related constants
# 0x20-0x2f reserved for logtype-related constants
# 0x30-0x3f reserved for timestamp-related constants
# Bytes are more convenient for encoding, and int for decoding due to how
# bytes/bytearrays work in python
ID_MASK: int = 0xF0
ID_VAR: int = 0x10
ID_LOGTYPE: int = 0x20
ID_TIMESTAMP: int = 0x30

VAR_STR_LEN_UBYTE: bytes = b"\x11"
VAR_STR_LEN_USHORT: bytes = b"\x12"
VAR_STR_LEN_INT: bytes = b"\x13"
VAR_COMPACT_ENCODING: bytes = b"\x18"

LOGTYPE_STR_LEN_UBYTE: bytes = b"\x21"
LOGTYPE_STR_LEN_USHORT: bytes = b"\x22"
LOGTYPE_STR_LEN_INT: bytes = b"\x23"

TIMESTAMP_DELTA_BYTE: bytes = b"\x31"
TIMESTAMP_DELTA_SHORT: bytes = b"\x32"
TIMESTAMP_DELTA_INT: bytes = b"\x33"
TIMESTAMP_NULL: bytes = b"\x3f"

# Convenience to avoid if/else statements everywhere
# Tuple contains size in bytes and True if the size is signed
# For compact encoding, size is the size of the variable itself rather than the
# size of the type storing the length value
SIZEOF: Dict[bytes, Tuple[int, bool]] = {
    VAR_COMPACT_ENCODING: (SIZEOF_INT, False),  # sign depends on int/float
    VAR_STR_LEN_UBYTE: (SIZEOF_BYTE, False),
    VAR_STR_LEN_USHORT: (SIZEOF_SHORT, False),
    VAR_STR_LEN_INT: (SIZEOF_INT, True),
    LOGTYPE_STR_LEN_UBYTE: (SIZEOF_BYTE, False),
    LOGTYPE_STR_LEN_USHORT: (SIZEOF_SHORT, False),
    LOGTYPE_STR_LEN_INT: (SIZEOF_INT, True),
    TIMESTAMP_DELTA_BYTE: (SIZEOF_BYTE, True),
    TIMESTAMP_DELTA_SHORT: (SIZEOF_SHORT, True),
    TIMESTAMP_DELTA_INT: (SIZEOF_INT, True),
}
