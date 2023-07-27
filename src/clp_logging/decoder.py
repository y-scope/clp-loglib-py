import json
from typing import Optional, Tuple

from clp_logging.protocol import (
    BYTE_ORDER,
    EOF_CHAR,
    ID_EOF,
    ID_LOGTYPE,
    ID_MASK,
    ID_TIMESTAMP,
    ID_VAR,
    MAGIC_NUMBER_COMPACT_ENCODING,
    Metadata,
    METADATA_JSON_ENCODING,
    METADATA_LEN_UBYTE,
    METADATA_LEN_USHORT,
    SIZEOF,
    SIZEOF_BYTE,
    SIZEOF_SHORT,
    VAR_COMPACT_ENCODING,
)


class CLPDecoder:
    """
    Namespace for all CLP decoding functions.

    Functions decode bytes extracting CLP tokens and translate them to their
    true type and value.
    """

    @staticmethod
    def decode_int(token: bytes) -> Tuple[int, str]:
        """:return: Tuple of `float` and `str` value of token"""
        var_int: int = int.from_bytes(token, BYTE_ORDER, signed=True)
        return var_int, str(var_int)

    @staticmethod
    def decode_float(token: bytes) -> Tuple[float, str]:
        """
        See the clp_ffi_py repo and CLP core repo for detailed breakdown of CLP
        float encoding.

        :return: Tuple of `float` and `str` value of token
        """
        backing_int: int = int.from_bytes(token, BYTE_ORDER)
        decimal_pos: int = (backing_int & 0x7) + 1
        backing_int >>= 3
        digit_count: int = (backing_int & 0x7) + 1
        backing_int >>= 3
        digits: int = backing_int & 0x1FFFFFF
        backing_int >>= 25
        negative: int = backing_int & 0x1

        var_float: float = digits / (10**decimal_pos)
        if negative:
            var_float *= -1

        var_str: str = str(digits).zfill(digit_count)
        var_str = var_str[:-decimal_pos] + "." + var_str[-decimal_pos:]
        if negative:
            var_str = "-" + var_str

        return var_float, var_str

    @staticmethod
    def decode_dict(token: bytes) -> str:
        return token.decode()

    @staticmethod
    def decode_preamble(src: bytes, pos: int) -> Tuple[Optional[Metadata], int]:
        """
        Decode `src` start at `pos` extracting the preamble/metadata.

        :return: On success returns the metadata and the index read up to in
        `src` (including `pos`). On error returns None and negative int based
        on error: -1 if magic number mismatch, -2 if metadata not in json
        """
        view: memoryview = memoryview(src[pos:])

        magic_len: int = len(MAGIC_NUMBER_COMPACT_ENCODING)
        if not view[pos : pos + magic_len] == MAGIC_NUMBER_COMPACT_ENCODING:
            return None, -1
        pos += magic_len
        encoding_len: int = len(METADATA_JSON_ENCODING)
        if not view[pos : pos + encoding_len] == METADATA_JSON_ENCODING:
            return None, -2
        pos += encoding_len

        type_byte: bytes = view[pos : pos + SIZEOF_BYTE]
        pos += SIZEOF_BYTE
        json_size: int
        if type_byte == METADATA_LEN_UBYTE:
            json_size = int.from_bytes(view[pos : pos + SIZEOF_BYTE], BYTE_ORDER)
            pos += SIZEOF_BYTE
        elif type_byte == METADATA_LEN_USHORT:
            json_size = int.from_bytes(view[pos : pos + SIZEOF_SHORT], BYTE_ORDER)
            pos += SIZEOF_SHORT
        else:
            raise NotImplementedError("Metadata length > unsigned short currently unsupported")

        metadata: Metadata = json.loads(view[pos : pos + json_size].tobytes())
        return metadata, pos + json_size

    @staticmethod
    def decode_token(src: memoryview, pos: int = 0) -> Tuple[int, bytes, int]:
        """
        Decode `src` start at `pos` extracting the next token.

        Note, we can use negative values despite `token_type` being an `int` as
        the integer value of an individual byte is restricted to [0:256) (see
        `bytes`). Therefore `token_type` (`type_byte[0]`) can never be negative.

        :param src: Bytes to decode
        :param pos: The position in `src` to begin decoding from
        :return: A tuple of the token type byte, the token in bytes, and the
        index read up to in `src` (including `pos`). Token type is 0 for
        EOF_CHAR, -1 if `src` is exhausted before completing a token, or < -1
        for other errors.
        """
        # We cannot directly get a single byte at pos with src[pos] as this
        # will return an integer and we still need to do byte comparisons
        # later.
        type_byte: memoryview = src[pos : pos + SIZEOF_BYTE]
        pos += SIZEOF_BYTE
        if type_byte == EOF_CHAR:
            return ID_EOF, type_byte, pos

        info: Optional[Tuple[int, bool]] = SIZEOF.get(type_byte.tobytes())
        if not info:
            return -3, type_byte, pos

        size: int
        signed: bool
        size, signed = info
        end: int = pos + size
        src_len: int = len(src)
        if end >= src_len:
            return -1, type_byte, end

        # SIZEOF_BYTE can only ever be 1 to match the size of an element in a
        # bytes-like python object. Therefore, we can get the integer value of
        # type_byte by indexing it.
        token_type: int = type_byte[0]
        token_id: int = token_type & ID_MASK
        if token_id == ID_VAR:
            if type_byte == VAR_COMPACT_ENCODING:
                return token_type, src[pos:end], end
            else:
                var_size: int = int.from_bytes(src[pos:end], BYTE_ORDER, signed=signed)
                pos = end
                end += var_size
                if end >= src_len:
                    return -1, type_byte, end
                return token_type, src[pos:end], end
        elif token_id == ID_LOGTYPE:
            logtype_size: int = int.from_bytes(src[pos:end], BYTE_ORDER, signed=signed)
            pos = end
            end += logtype_size
            if end >= src_len:
                return -1, type_byte, end
            return token_type, src[pos:end], end
        elif token_id == ID_TIMESTAMP:
            return token_type, src[pos:end], end
        else:
            return -2, type_byte, end
