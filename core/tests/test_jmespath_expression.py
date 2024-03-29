from datetime import datetime, timedelta, timezone
from typing import Any, Dict

import pytest
from datayoga_core.expression import JMESPathExpression

expression = JMESPathExpression()


def test_jmespath_concat_expression():
    expression.compile("concat([fname,' ',mname,' ',lname])")
    assert expression.search({"fname": "John", "mname": "George", "lname": "Smith"}) == "John George Smith"


def test_jmespath_capitalize_expression():
    expression.compile("capitalize(name)")
    assert expression.search({"name": "john george smith"}) == "John George Smith"


def test_jmespath_lower_expression():
    expression.compile("lower(product)")
    assert expression.search({"product": "PhonE"}) == "phone"


def test_jmespath_capitalize_expression_null():
    expression.compile("capitalize(name)")
    assert expression.search({"name": None}) is None


def test_jmespath_lower_expression_null():
    expression.compile("lower(product)")
    assert expression.search({"product": None}) is None


def test_jmespath_upper_expression():
    expression.compile("upper(product)")
    assert expression.search({"product": "PhonE"}) == "PHONE"


def test_jmespath_upper_expression_null():
    expression.compile("upper(product)")
    assert expression.search({"product": None}) is None


def test_jmespath_replace_empty_string():
    expression.compile("replace(sentence, '', 'two')")
    assert expression.search({"sentence": "one four three four!"}) == "one four three four!"


def test_jmespath_replace_expression():
    expression.compile("replace(sentence, 'four', 'two')")
    assert expression.search({"sentence": "one four three four!"}) == "one two three two!"


def test_jmespath_replace_expression_null():
    expression.compile("replace(sentence, 'four', 'two')")
    assert expression.search({"sentence": None}) is None


def test_jmespath_right_expression():
    expression.compile("right(sentence, `5`)")
    assert expression.search({"sentence": "one four three four!"}) == "four!"


def test_jmespath_right_expression_null():
    expression.compile("right(sentence, `5`)")
    assert expression.search({}) is None


def test_jmespath_left_expression():
    expression.compile("left(sentence, `3`)")
    assert expression.search({"sentence": "one four three four!"}) == "one"


def test_jmespath_left_expression_null():
    expression.compile("left(sentence, `5`)")
    assert expression.search({}) is None


def test_jmespath_mid_expression():
    expression.compile("mid(sentence, `3`, `7`)")
    assert expression.search({"sentence": "one four three four!"}) == " four t"


def test_jmespath_mid_expression_null():
    expression.compile("mid(sentence, `1`, `4`)")
    assert expression.search({}) is None


def test_jmespath_split_expression():
    expression.compile("split(departments)")
    assert expression.search({"departments": "finance,hr,r&d"}) == ["finance", "hr", "r&d"]


def test_jmespath_split_expression_null():
    expression.compile("split(animals, ',')")
    assert expression.search({"animals": None}) is None


def test_jmespath_split_delimiter_expression():
    expression.compile("split(animals, '|')")
    assert expression.search({"animals": "cat|dog|frog|mouse"}) == ["cat", "dog", "frog", "mouse"]


def test_jmespath_uuid():
    expression.compile("uuid()")
    val = expression.search(None)

    assert len(val) == 36
    assert isinstance(val, str)
    assert val[8] == "-"
    assert val[13] == "-"
    assert val[13] == "-"
    assert val[18] == "-"
    assert val[23] == "-"


def test_jmespath_hash():
    # Test default hash(sha1)
    expression.compile("hash(null)")
    assert expression.search(None) == "da39a3ee5e6b4b0d3255bfef95601890afd80709"

    expression.compile("hash(some_null)")
    assert expression.search({"some_null": None}) == "da39a3ee5e6b4b0d3255bfef95601890afd80709"

    expression.compile("hash(some_arr)")
    assert expression.search({"some_arr": [1, 2, "3", {}]}) == "55ff109a5a35588b4128d36b7806f3cbcd4f59f2"

    expression.compile("hash(some_str)")
    assert expression.search({"some_str": "some_value"}) == "8c818171573b03feeae08b0b4ffeb6999e3afc05"

    expression.compile("hash(some_bool)")
    assert expression.search({"some_bool": True}) == "5ffe533b830f08a0326348a9160afafc8ada44db"

    expression.compile("hash(some_obj)")
    assert expression.search({"some_obj": {"some_inner": "some_value"}}) == "c756dab1743ae081dc9214a837a1ed4a4c341e5a"

    #  Test hashes comes from empty string
    hashes_null = (
        ('sha256', 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'),
        ('md5', 'd41d8cd98f00b204e9800998ecf8427e'),
        ('sha384', '38b060a751ac96384cd9327eb1b1e36a21fdb71114be07434c0cc7bf63f6e1da274edebfe76f65fbd51ad2f14898b95b'),
        ('sha3_384', '0c63a75b845e4f7d01107d852e4c2485c51a50aaaa94fc61'
                     '995e71bbee983a2ac3713831264adb47fb6bd1e058d5f004'),
        ('blake2b', '786a02f742015903c6c6fd852552d272912f4740e15847618a86e217f71f5419d'
                    '25e1031afee585313896444934eb04b903a685b1448b755d56f701afe9be2ce'),
        ('sha1', 'da39a3ee5e6b4b0d3255bfef95601890afd80709'),
        ('sha512', 'cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce'
                   '47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e'),
        ('sha3_224', '6b4e03423667dbb73b6e15454f0eb1abd4597f9a1b078e3f5b5a6bc7'),
        ('sha224', 'd14a028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f'),
        ('sha3_256', 'a7ffc6f8bf1ed76651c14756a061d662f580ff4de43b49fa82d80a4b80f8434a'),
        ('sha3_512', 'a69f73cca23a9ac5c8b567dc185a756e97c982164fe25859e0d1dcc1475c80a6'
                     '15b2123af1f5f94c11e3e9402c3ac558f500199d95b6d3e301758586281dcd26'),
        ('blake2s', '69217a3079908094e11121d042354a7c1f55b6482ca1a51e1b250dfd1ed0eef9'),
    )

    for alg, val in hashes_null:
        expression.compile(f"hash(null, `{alg}`)")
        assert expression.search(None) == val


def test_jmespath_time_delta_days():
    expression.compile("time_delta_days(dt)")

    dt = datetime.now(tz=timezone.utc) - timedelta(days=365)
    assert expression.search({"dt": dt.isoformat()}) == 365

    dt = datetime.now() - timedelta(days=-365)
    assert expression.search({"dt": dt.isoformat()}) == -365

    dt = datetime.now(tz=timezone.utc) - timedelta(days=10)
    assert expression.search({"dt": dt.timestamp()}) == 10

    dt = datetime.now() - timedelta(days=-10)
    assert expression.search({"dt": dt.timestamp()}) == -10


def test_jmespath_time_delta_seconds():
    expression.compile("time_delta_seconds(dt)")

    dt = datetime.now(tz=timezone.utc) - timedelta(seconds=31557600)
    assert expression.search({"dt": dt.isoformat()}) == 31557600

    dt = datetime.now() - timedelta(seconds=-31546800)
    assert expression.search({"dt": dt.isoformat()}) == -31546800

    dt = datetime.now(tz=timezone.utc) - timedelta(days=10)
    assert expression.search({"dt": dt.timestamp()}) == 864000

    dt = datetime.now() - timedelta(days=-10)
    assert expression.search({"dt": dt.timestamp()}) == -864000


def test_regex_replace():
    expression.compile("regex_replace(text, pattern, replacement)")

    assert expression.search({"text": "Banana Bannnana", "pattern": r"Ban\w+", "replacement": "Apple"}) == "Apple Apple"
    assert expression.search({"text": "Bana\nBannnana", "pattern": r"Ban\w+", "replacement": "Apple"}) == "Apple\nApple"

    expression.compile(r"regex_replace(text, 'Ban\w+', 'Apple')")

    assert expression.search({"text": "Banana Bannnana"}) == "Apple Apple"
    assert expression.search({"text": "Bana\nBannnana"}) == "Apple\nApple"


def test_jmespath_in():
    expression.compile("in(el, itr)")

    assert expression.search({"el": 2, "itr": [1, 2, 3, 4]})
    assert expression.search({"el": "c", "itr": ["a", "b", "c", "d"]})

    assert not expression.search({"el": 0, "itr": [1, 2, 3, 4]})
    assert not expression.search({"el": "x", "itr": ["a", "b", "c", "d"]})

    expression.compile("in(el, `[1, 2, 3, 4, 5]`)")

    assert expression.search({"el": 1})
    assert not expression.search({"el": 0})


def test_jmespath_nested_expression():
    expression.compile("a.b")
    assert expression.search({"a": {"b": "c"}}) == "c"


def test_jmespath_filter():
    expression.compile("fname==`a`")
    assert expression.filter(
        [{"fname": "a", "id": 1},
         {"fname": "b", "id": 2},
         {"fname": "a", "id": 3}]) == [
        {"fname": "a", "id": 1},
        {"fname": "a", "id": 3}]


def test_jmespath_filter_tombstone():
    expression.compile("fname==`a`")
    assert expression.filter(
        [{"fname": "a", "id": 1},
         {"fname": "b", "id": 2},
         {"fname": "a", "id": 3}], tombstone=True) == [
        {"fname": "a", "id": 1},
        None,
        {"fname": "a", "id": 3}]


def test_jmespath_json_parse():
    expression.compile("json_parse(data)")
    assert expression.search({"data": """{"greeting": "hello world!"}"""}) == {"greeting": "hello world!"}


def test_jmespath_base64_decode():
    expression.compile("base64_decode(data)")
    assert expression.search({"data": "SGVsbG8gV29ybGQh"}) == "Hello World!"
    assert expression.search({"data": "X19kZWJleml1bV91bmF2YWlsYWJsZV92YWx1ZQ=="}) == "__debezium_unavailable_value"


def test_jmespath_to_entries():
    """Test the `to_entries` custom function."""
    expression.compile("to_entries(obj)")

    data = {"name": "John", "age": 30, "city": "New York"}
    expected_result = [
        {"key": "name", "value": "John"},
        {"key": "age", "value": 30},
        {"key": "city", "value": "New York"},
    ]

    assert expression.search({"obj": data}) == expected_result

    # Test with an empty object
    assert expression.search({"obj": {}}) == []

    # Test with a None value
    assert expression.search({"obj": None}) is None


def test_jmespath_from_entries():
    """Test the `from_entries` custom function."""
    expression.compile("from_entries(entries)")

    entries = [
        {"key": "name", "value": "John"},
        {"key": "age", "value": 30},
        {"key": "city", "value": "New York"},
    ]

    expected_result = {"name": "John", "age": 30, "city": "New York"}

    assert expression.search({"entries": entries}) == expected_result

    # Test with an empty array
    assert expression.search({"entries": []}) == {}

    # Test with a None value
    assert expression.search({"entries": None}) is None


@pytest.mark.parametrize("jmespath_expression",
                         ["to_entries(@)[?value!=null] | from_entries(@)", "filter_entries(@, `value!=null`)"])
def test_jmespath_remove_null_values_from_entries(jmespath_expression: str):
    """Test the expression to remove null values from entries."""
    expression.compile(jmespath_expression)

    data = {
        "name": "John",
        "age": 30,
        "city": None,
        "country": "USA",
        "email": None
    }

    expected_result = {
        "name": "John",
        "age": 30,
        "country": "USA"
    }

    assert expression.search(data) == expected_result

    # Test with an empty object
    assert expression.search({}) == {}

    # Test with a None value
    assert expression.search(None) is None


@pytest.mark.parametrize("jmespath_expression, expected_result", [
    ("filter_entries(@, `key == 'name' || key == 'age'`)", {"name": "John", "age": 30}),
    ("filter_entries(@, `in(value, \\`[15, 30]\\`)`)", {"age": 30, "score": 15}),
    ("filter_entries(@, `value > \\`20\\``)", {"age": 30}),
    ("filter_entries(@, `starts_with(key, \\`country\\`)`)", {"country_code": 1, "country": "USA"})
])
def test_jmespath_filter_entries(jmespath_expression: str, expected_result: Dict[str, Any]):
    expression.compile(jmespath_expression)
    data = {
        "name": "John",
        "age": 30,
        "country_code": 1,
        "country": "USA",
        "email": None,
        "score": 15
    }

    assert expression.search(data) == expected_result

    # Test with an empty object
    assert expression.search({}) == {}

    # Test with a None value
    assert expression.search(None) is None
