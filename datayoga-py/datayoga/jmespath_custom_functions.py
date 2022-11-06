import hashlib
import string
from datetime import datetime, timezone
from typing import Union
from uuid import uuid4

import json
from jmespath import functions


# custom functions for Jmespath
class JmespathCustomFunctions(functions.Functions):

    @functions.signature({"types": ["string", "null"]})
    def _func_capitalize(self, arg):
        return string.capwords(str(arg)) if arg is not None else None

    @functions.signature({"types": ["array"]})
    def _func_concat(self, elements):
        return ''.join([str(x) for x in elements])

    @functions.signature({"types": ["string", "null"]})
    def _func_lower(self, element):
        return str(element).lower() if element is not None else None

    @functions.signature({"types": ["string", "null"]})
    def _func_upper(self, element):
        return str(element).upper() if element is not None else None

    @functions.signature({"types": ["string", "null"]}, {"types": ["string"]}, {"types": ["string"]})
    def _func_replace(self, element, old_value, new_value):
        return str(element).replace(old_value, new_value) if element is not None else None

    @functions.signature({"types": ["string", "null"]}, {"types": ["number"]})
    def _func_right(self, element, amount):
        return str(element)[-amount:] if element is not None else None

    @functions.signature({"types": ["string", "null"]}, {"types": ["number"]})
    def _func_left(self, element, amount):
        return str(element)[:amount] if element is not None else None

    @functions.signature({"types": ["string", "null"]}, {"types": ["number"]}, {"types": ["number"]})
    def _func_mid(self, element, offset, amount):
        return str(element)[offset:offset+amount] if element is not None else None

    @functions.signature({"types": ["string", "null"], "variadic": True})
    def _func_split(self, element, delimiter=","):
        return str(element).split(delimiter) if element is not None else None

    @functions.signature()
    def _func_uuid(self):
        """Generates a random UUID4 and returns it as a string in standard format."""

        return str(uuid4())

    @functions.signature({"types": ["number", "string", "boolean", "array", "object", "null"], "variadic": True})
    def _func_hash(self, obj, hash_name="sha1"):
        """\
        Calculates a hash using given the `hash_name` hash function and returns its hexadecimal representation.

        Supported algorithms:

        - sha1(default)
        - sha256
        - md5
        - sha384
        - sha3_384
        - blake2b
        - sha512
        - sha3_224
        - sha224
        - sha3_256
        - sha3_512
        - blake2s

        See https://docs.python.org/3/library/hashlib.html for more information.
        """

        def prepare() -> Union[bytes, bytearray]:
            if isinstance(obj, (bytes, bytearray)):
                return obj

            if obj is None:
                return b""

            if isinstance(obj, str):
                return obj.encode()

            # the 'separators' arg is needed to remove whitespace in the resulting string
            return json.dumps(obj, separators=(',', ':')).encode("utf-8")

        h = hashlib.new(hash_name)
        h.update(prepare())
        return h.hexdigest()

    @functions.signature({"types": ["string", "number"]})
    def _func_time_delta_days(self, dt):
        """\
        Returns the number of days between given `dt` and now (positive)
        or the number of days that have passed from now (negative).

        If `dt` is a string, ISO datetime (2011-11-04T00:05:23+04:00, for example) is assumed.
        If `dt` is a number, Unix timestamp (1320365123, for example) is assumed.
        """

        dt = datetime.fromisoformat(dt) if isinstance(dt, str) else datetime.fromtimestamp(dt, timezone.utc)
        delta = dt.now(dt.tzinfo) - dt
        return delta.days

    @functions.signature({"types": ["string", "number"]})
    def _func_time_delta_seconds(self, dt):
        """\
        Returns the number of seconds between given `dt` and now (positive)
        or the number of seconds that have passed from now (negative).

        If `dt` is a string, ISO datetime (2011-11-04T00:05:23+04:00, for example) is assumed.
        If `dt` is a number, Unix timestamp (1320365123, for example) is assumed.
        """

        dt = datetime.fromisoformat(dt) if isinstance(dt, str) else datetime.fromtimestamp(dt, timezone.utc)
        delta = dt.now(dt.tzinfo) - dt

        return delta.days * 86400 + delta.seconds
