from datayoga.blocks.expression import JMESPathExpression

expression = JMESPathExpression()

CONCAT_EXPRESSION = "concat([fname,' ',mname,' ',lname])"
TEST_JSON = {"fname": "JohN",  "mname": "GeorgE", "lname": "SmitH"}


def test_jmespath_concat_expression():
    expression.compile(CONCAT_EXPRESSION)
    assert expression.search(TEST_JSON) == "JohN GeorgE SmitH"


def test_jmespath_capitalize_expression():
    expression.compile(f"capitalize({CONCAT_EXPRESSION})")
    assert expression.search(TEST_JSON) == "John George Smith"


def test_jmespath_lower_expression():
    expression.compile(f"lower({CONCAT_EXPRESSION})")
    assert expression.search(TEST_JSON) == "john george smith"


def test_jmespath_upper_expression():
    expression.compile(f"upper({CONCAT_EXPRESSION})")
    assert expression.search(TEST_JSON) == "JOHN GEORGE SMITH"
