from datayoga.blocks.expression import JMESPathExpression


def test_sql_expression():
    expression = JMESPathExpression()
    expression.compile("concat([fname,' ',mname,' ',lname])")
    assert expression.search({"fname": "john",  "mname": "george", "lname": "smith"}) == "john george smith"
