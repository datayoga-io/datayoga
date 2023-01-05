from datayoga_core.expression import SQLExpression


def test_sql_expression():
    sql_expression = SQLExpression()
    sql_expression.compile("fname || ' ' || mname || ' ' || lname")
    assert sql_expression.search({"fname": "john",  "mname": "george", "lname": "smith"}) == "john george smith"

def test_sql_expression_number_literal():
    sql_expression = SQLExpression()
    sql_expression.compile("10")
    assert sql_expression.search({"fname": "john",  "mname": "george", "lname": "smith"}) == 10

def test_sql_expression_string_literal():
    sql_expression = SQLExpression()
    sql_expression.compile("'10'")
    assert sql_expression.search({"fname": "john",  "mname": "george", "lname": "smith"}) == "10"

def test_sql_expression_list():
    sql_expression = SQLExpression()
    sql_expression.compile("fname || ' ' || mname || ' ' || lname")
    assert sql_expression.search_bulk([{"fname": "john",  "mname": "george", "lname": "smith"}]) == ["john george smith"]


def test_sql_expression_multiple_fields():
    sql_expression = SQLExpression()
    sql_expression.compile('{"fullname":"fname || \' \' || lname", "fname":"upper(fname)"}')
    assert sql_expression.search_bulk([{"fname": "john",  "mname": "george", "lname": "smith"}]) == [
        {"fullname": "john smith", "fname": "JOHN"}]


def test_sql_expression_nested_fields():
    sql_expression = SQLExpression()
    sql_expression.compile('`before.fname` || \' \' || `after.lname`')
    assert sql_expression.search_bulk([{"before": {"fname": "john"}, "after": {"lname": "smith"}}]) == ["john smith"]


def test_sql_filter():
    sql_expression = SQLExpression()
    sql_expression.compile("fname='a'")
    assert sql_expression.filter(
        [{"fname": "a", "id": 1},
         {"fname": "b", "id": 2},
         {"fname": "a", "id": 3}]) == [
        {"fname": "a", "id": 1},
        {"fname": "a", "id": 3}]

def test_sql_filter_tombstone():
    sql_expression = SQLExpression()
    sql_expression.compile("fname='a'")
    assert sql_expression.filter(
        [{"fname": "a", "id": 1},
         {"fname": "b", "id": 2},
         {"fname": "a", "id": 3}],tombstone=True) == [
        {"fname": "a", "id": 1},
        None,
        {"fname": "a", "id": 3}]

def test_sql_expression_batch():
    sql_expression = SQLExpression()
    sql_expression.compile('{"fullname":"fname || \' \' || lname", "fname":"upper(fname)"}')
    assert sql_expression.search_bulk([
        {"fname": "john",  "mname": "george", "lname": "smith"},
        {"fname": "john2",  "mname": "george2", "lname": "smith2"},
        ]) == [
            {"fullname": "john smith", "fname": "JOHN"},
            {"fullname": "john2 smith2", "fname": "JOHN2"}
        ]
