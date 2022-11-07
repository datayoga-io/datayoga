from datetime import datetime, timedelta, timezone

from datayoga_core.expression import JMESPathExpression

expression = JMESPathExpression()


def test_jmespath_concat_expression():
    expression.compile("concat([fname,' ',mname,' ',lname])")
    assert expression.search({"fname": "John", "mname": "George", "lname": "Smith"}) == "John George Smith"


def test_jmespath_capitalize_expression():
    expression.compile("capitalize(name)")
    assert expression.search({"name": "john george smith"}) == "John George Smith"


def test_jmespath_lower_expression():
    expression.compile(f"lower(product)")
    assert expression.search({"product": "PhonE"}) == "phone"


def test_jmespath_capitalize_expression_null():
    expression.compile("capitalize(name)")
    assert expression.search({"name": None}) == None


def test_jmespath_lower_expression_null():
    expression.compile(f"lower(product)")
    assert expression.search({"product": None}) == None


def test_jmespath_upper_expression():
    expression.compile(f"upper(product)")
    assert expression.search({"product": "PhonE"}) == "PHONE"


def test_jmespath_upper_expression_null():
    expression.compile(f"upper(product)")
    assert expression.search({"product": None}) == None


def test_jmespath_replace_expression():
    expression.compile(f"replace(sentence, 'four', 'two')")
    assert expression.search({"sentence": "one four three four!"}) == "one two three two!"


def test_jmespath_replace_expression_null():
    expression.compile(f"replace(sentence, 'four', 'two')")
    assert expression.search({"sentence": None}) == None


def test_jmespath_right_expression():
    expression.compile(f"right(sentence, `5`)")
    assert expression.search({"sentence": "one four three four!"}) == "four!"


def test_jmespath_right_expression_null():
    expression.compile(f"right(sentence, `5`)")
    assert expression.search({}) == None


def test_jmespath_left_expression():
    expression.compile(f"left(sentence, `3`)")
    assert expression.search({"sentence": "one four three four!"}) == "one"


def test_jmespath_left_expression_null():
    expression.compile(f"left(sentence, `5`)")
    assert expression.search({}) == None


def test_jmespath_mid_expression():
    expression.compile(f"mid(sentence, `3`, `7`)")
    assert expression.search({"sentence": "one four three four!"}) == " four t"


def test_jmespath_mid_expression_null():
    expression.compile(f"mid(sentence, `1`, `4`)")
    assert expression.search({}) == None


def test_jmespath_split_expression():
    expression.compile(f"split(departments)")
    assert expression.search({"departments": "finance,hr,r&d"}) == ["finance", "hr", "r&d"]


def test_jmespath_split_expression_null():
    expression.compile(f"split(animals, ',')")
    assert expression.search({"animals": None}) == None


def test_jmespath_split_delimiter_expression():
    expression.compile(f"split(animals, '|')")
    assert expression.search({"animals": "cat|dog|frog|mouse"}) == ["cat", "dog", "frog", "mouse"]


def test_jmespath_uuid():
    expression.compile(f"uuid()")
    val = expression.search(None)

    assert len(val) == 36
    assert isinstance(val, str)
    assert val[8] == '-'
    assert val[13] == '-'
    assert val[13] == '-'
    assert val[18] == '-'
    assert val[23] == '-'


def test_jmespath_hash():
    # Test default hash(sha1)
    expression.compile(f"hash(null)")
    assert expression.search(None) == 'da39a3ee5e6b4b0d3255bfef95601890afd80709'

    expression.compile(f"hash(some_null)")
    assert expression.search({"some_null": None}) == 'da39a3ee5e6b4b0d3255bfef95601890afd80709'

    expression.compile(f"hash(some_arr)")
    assert expression.search({"some_arr": [1, 2, "3", {}]}) == '55ff109a5a35588b4128d36b7806f3cbcd4f59f2'

    expression.compile(f"hash(some_str)")
    assert expression.search({"some_str": "some_value"}) == '8c818171573b03feeae08b0b4ffeb6999e3afc05'

    expression.compile(f"hash(some_bool)")
    assert expression.search({"some_bool": True}) == '5ffe533b830f08a0326348a9160afafc8ada44db'

    expression.compile(f"hash(some_obj)")
    assert expression.search({"some_obj": {"some_inner": "some_value"}}) == 'c756dab1743ae081dc9214a837a1ed4a4c341e5a'

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
    expression.compile(f"time_delta_days(dt)")

    dt = datetime.now(tz=timezone.utc) - timedelta(days=365)
    assert expression.search({"dt": dt.isoformat()}) == 365

    dt = datetime.now() - timedelta(days=-365)
    assert expression.search({"dt": dt.isoformat()}) == -365

    dt = datetime.now(tz=timezone.utc) - timedelta(days=10)
    assert expression.search({"dt": dt.timestamp()}) == 10

    dt = datetime.now() - timedelta(days=-10)
    assert expression.search({"dt": dt.timestamp()}) == -10


def test_jmespath_time_delta_seconds():
    expression.compile(f"time_delta_seconds(dt)")

    dt = datetime.now(tz=timezone.utc) - timedelta(seconds=31557600)
    assert expression.search({"dt": dt.isoformat()}) == 31557600

    dt = datetime.now() - timedelta(seconds=-31546800)
    assert expression.search({"dt": dt.isoformat()}) == -31546800

    dt = datetime.now(tz=timezone.utc) - timedelta(days=10)
    assert expression.search({"dt": dt.timestamp()}) == 864000

    dt = datetime.now() - timedelta(days=-10)
    assert expression.search({"dt": dt.timestamp()}) == -864000
