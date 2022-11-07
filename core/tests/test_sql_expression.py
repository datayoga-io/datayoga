from datayoga_core.expression import SQLExpression


def test_sql_expression():
    sql_expression = SQLExpression()
    sql_expression.compile("fname || ' ' || mname || ' ' || lname")
    assert sql_expression.search([{"fname": "john",  "mname": "george", "lname": "smith"}]) == "john george smith"


def test_sql_expression_multiple_fields():
    sql_expression = SQLExpression()
    sql_expression.compile('{"fullname":"fname || \' \' || lname", "fname":"upper(fname)"}')
    assert sql_expression.search([{"fname": "john",  "mname": "george", "lname": "smith"}]) == {
        "fullname": "john smith", "fname": "JOHN"}
