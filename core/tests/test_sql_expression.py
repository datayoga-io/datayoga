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

def test_sql_expression_nested_fields():
    sql_expression = SQLExpression()
    sql_expression.compile('`before.fname` || \' \' || `after.lname`')
    assert sql_expression.search([{"before":{"fname": "john"}, "after":{ "lname": "smith"}}]) == "john smith"

def test_sql_filter():
    sql_expression = SQLExpression()
    sql_expression.compile("fname='a'")
    assert sql_expression.filter([{"fname":"a","id":1},{"fname":"b","id":2},{"fname":"a","id":3}]) == [{"fname":"a","id":1},{"fname":"a","id":3}]
