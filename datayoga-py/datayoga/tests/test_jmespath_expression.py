from datayoga.expression import JMESPathExpression

expression = JMESPathExpression()


def test_jmespath_concat_expression():
    expression.compile("concat([fname,' ',mname,' ',lname])")
    assert expression.search({"fname": "John",  "mname": "George", "lname": "Smith"}) == "John George Smith"


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
