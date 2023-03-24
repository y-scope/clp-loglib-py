import json
from typing import Dict, Optional, Tuple

from clp_logging.protocol import (
    BYTE_ORDER,
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
        See `CLPEncoder.encode_float` for detailed breakdown of CLP float
        encoding.
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

        type_byte: bytes = view[pos : pos + 1]
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
        `bytes`). Therefore `token_type` (`type_byte[0]`) can never be
        negative.

        :param src: Bytes to decode
        :param pos: The position in `src` to begin decoding from
        :return: A tuple of the token type byte, the token in bytes, and the
        index read up to in `src` (including `pos`). Token type is 0 for
        EOF_CHAR, -1 if `src` is exhausted before completing a token, -2 if
        it is exhausted with a null byte, or < -2 for other errors.
        """
        type_byte: memoryview = src[pos : pos + 1]
        token_type: int = type_byte[0]
        token_id: int = token_type & ID_MASK
        pos += SIZEOF_BYTE
        src_len: int = len(src)
        if token_id == ID_EOF:
            if src_len == pos:
                # This is a single null byte, which can indicate the actual EOF,
                # or it might be a partial state received from the socket.
                # Use a unique return value to flag this case,
                # and leave the caller to make the judgement.
                return -2, type_byte, pos + 1
            # Calling `zstream.flush(FLUSH_FRAME)` seems to leave
            # \x00\x00\x00\xc9 between frames.
            if pos + 4 >= src_len:
                return -1, type_byte, pos + 4
            if b"\x00\x00\x00\xc9" == src[pos : pos + 4]:
                return token_type, type_byte, pos + 4
            else:
                return -5, type_byte, pos + 1

        info: Optional[Tuple[int, bool]] = SIZEOF.get(type_byte.tobytes())
        if not info:
            return -3, type_byte, pos

        size: int
        signed: bool
        size, signed = info
        end: int = pos + size
        if end >= src_len:
            return -1, type_byte, end

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
            return -4, type_byte, end
