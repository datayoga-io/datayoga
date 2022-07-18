from datayoga.blocks.expression import SQLExpression


def test_sql_expression():
    sql_expression = SQLExpression()
    sql_expression.compile("fname || ' ' || mname || ' ' || lname")
    assert sql_expression.search({"fname": "john",  "mname": "george", "lname": "smith"}) == "john george smith"
