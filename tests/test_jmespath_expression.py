from datayoga.blocks.expression import JMESPathExpression

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
