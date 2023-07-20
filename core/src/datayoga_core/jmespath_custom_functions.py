import base64
import hashlib
import re
import string
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Union
from uuid import uuid4

import jmespath
import orjson
from jmespath import functions


# custom functions for JMESPath
class JmespathCustomFunctions(functions.Functions):

    @functions.signature({"types": ["string", "null"]})
    def _func_capitalize(self, arg):
        return string.capwords(f"{arg}") if arg is not None else None

    @functions.signature({"types": ["array"]})
    def _func_concat(self, elements):
        return "".join([f"{x}" for x in elements])

    @functions.signature({"types": ["string", "null"]})
    def _func_lower(self, element):
        return f"{element}".lower() if element is not None else None

    @functions.signature({"types": ["string", "null"]})
    def _func_upper(self, element):
        return f"{element}".upper() if element is not None else None

    @functions.signature({"types": ["string", "null"]}, {"types": ["string"]}, {"types": ["string"]})
    def _func_replace(self, element, old_value, new_value):
        if element is None:
            return None

        # Unexpected behavior when `old_value` is an empty string in Python's builtin replace
        # https://bugs.python.org/issue28029
        return f"{element}".replace(old_value, new_value) if old_value != "" else f"{element}"

    @functions.signature({"types": ["string", "null"]}, {"types": ["number"]})
    def _func_right(self, element, amount):
        return f"{element}"[-amount:] if element is not None else None

    @functions.signature({"types": ["string", "null"]}, {"types": ["number"]})
    def _func_left(self, element, amount):
        return f"{element}"[:amount] if element is not None else None

    @functions.signature({"types": ["string", "null"]}, {"types": ["number"]}, {"types": ["number"]})
    def _func_mid(self, element, offset, amount):
        return f"{element}"[offset:offset+amount] if element is not None else None

    @functions.signature({"types": ["string", "null"], "variadic": True})
    def _func_split(self, element, delimiter=","):
        return f"{element}".split(delimiter) if element is not None else None

    @functions.signature()
    def _func_uuid(self):
        """Generates a random UUID4 and returns it as a string in standard format."""

        return str(uuid4())

    @functions.signature({"types": ["number", "string", "boolean", "array", "object", "null"], "variadic": True})
    def _func_hash(self, obj, hash_name="sha1"):
        """Calculates a hash using given the `hash_name` hash function and returns its hexadecimal representation.

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

            return orjson.dumps(obj, option=orjson.OPT_SORT_KEYS)

        h = hashlib.new(hash_name)
        h.update(prepare())
        return h.hexdigest()

    @functions.signature({"types": ["string", "number"]})
    def _func_time_delta_days(self, dt):
        """Returns the number of days between given `dt` and now (positive)
        or the number of days that have passed from now (negative).

        If `dt` is a string, ISO datetime (2011-11-04T00:05:23+04:00, for example) is assumed.
        If `dt` is a number, Unix timestamp (1320365123, for example) is assumed.
        """

        dt = datetime.fromisoformat(dt) if isinstance(dt, str) else datetime.fromtimestamp(dt, timezone.utc)
        delta = dt.now(dt.tzinfo) - dt
        return delta.days

    @functions.signature({"types": ["string", "number"]})
    def _func_time_delta_seconds(self, dt):
        """Returns the number of seconds between given `dt` and now (positive)
        or the number of seconds that have passed from now (negative).

        If `dt` is a string, ISO datetime (2011-11-04T00:05:23+04:00, for example) is assumed.
        If `dt` is a number, Unix timestamp (1320365123, for example) is assumed.
        """

        dt = datetime.fromisoformat(dt) if isinstance(dt, str) else datetime.fromtimestamp(dt, timezone.utc)
        delta = dt.now(dt.tzinfo) - dt

        return delta.days * 86400 + delta.seconds

    @functions.signature({"types": ["string"]}, {"types": ["string"]}, {"types": ["string"]})
    def _func_regex_replace(self, text: str, pattern: str, replacement: str) -> str:
        """Replaces matched patterns in the string by the given replacement."""
        return re.sub(pattern, replacement, text)

    @functions.signature({"types": ["number", "string", "boolean", "array", "object", "null"]}, {"types": ["array"]})
    def _func_in(self, element: Any, iterable: Iterable) -> bool:
        """Returns True if the iterable contains the given element."""
        return element in iterable

    @functions.signature({"types": ["string"]})
    def _func_json_parse(self, data: str) -> Any:
        """Returns parsed object from the given json string."""
        return orjson.loads(data)

    @functions.signature({"types": ["string"]})
    def _func_base64_decode(self, data: str) -> str:
        """Returns decoded string from the given base64 encoded string."""
        return base64.b64decode(data).decode()

    @functions.signature({"types": ["object", "null"]})
    def _func_to_entries(self, obj: Optional[Dict[str, Any]]) -> Optional[List[Dict[str, Any]]]:
        """
        Takes an object and returns an array of {key: key, value: value}.

        Args:
            obj (Optional[Dict[str, Any]]): The object to convert to entries.

        Returns:
            Optional[List[Dict[str, Any]]]: The array of {key: key, value: value} entries.
        """
        if obj is None:
            return None

        return [{"key": key, "value": value} for key, value in obj.items()]

    @functions.signature({"types": ["array", "null"]})
    def _func_from_entries(self, entries: Optional[List[Dict[str, Any]]]) -> Optional[Dict[str, Any]]:
        """
        Takes an array of {key: key, value: value} and returns an object.

        Args:
            entries (Optional[List[Dict[str, Any]]])): The list of entries to convert to an object.

        Returns:
           Optional[Dict[str, Any]]: The object created from the array of entries.
        """
        if entries is None:
            return None

        result = {}
        for entry in entries:
            if isinstance(entry, dict):
                key = entry.get("key")
                if key is not None:
                    result[key] = entry.get("value")

        return result

    @functions.signature({"types": ["object", "null"]}, {"types": ["string"]})
    def _func_filter_entries(self, entries: Optional[Dict[str, Any]],
                             predicate: str) -> Optional[Dict[str, Any]]:
        """
        Filters entries based on the given predicate.

        Args:
            entries (Optional[Dict[str, Any]]): The dictionary (object) of entries to filter.
            predicate (str): The JMESPath predicate to use for filtering.

        Returns:
            Optional[Dict[str, Any]]: The filtered dictionary (object) of entries.
        """
        if entries is None:
            return None

        # Register custom functions
        jmespath_options = jmespath.Options(custom_functions=JmespathCustomFunctions())

        # Compile the predicate expression
        compiled_predicate = jmespath.compile(predicate)

        # Apply the predicate to filter entries
        filtered_entries = {}
        for key, value in entries.items():
            try:
                if compiled_predicate.search({"key": key, "value": value}, options=jmespath_options):
                    filtered_entries[key] = value
            except Exception:
                pass

        return filtered_entries
