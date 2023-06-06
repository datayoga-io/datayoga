import datetime

import pytest
from datayoga_core.expression import SQLExpression


def test_sql_expression():
    sql_expression = SQLExpression()
    sql_expression.compile("fname || ' ' || mname || ' ' || lname")
    assert sql_expression.search({"fname": "john",  "mname": "george", "lname": "smith"}) == "john george smith"


def test_sql_expression_number_literal():
    sql_expression = SQLExpression()
    sql_expression.compile("10")
    assert sql_expression.search({"fname": "john",  "mname": "george", "lname": "smith"}) == 10


def test_sql_expression_single_date_function():
    sql_expression = SQLExpression()
    sql_expression.compile("current_date")
    assert sql_expression.search({"fname": "john",  "mname": "george", "lname": "smith"}
                                 ) == datetime.datetime.now().strftime("%Y-%m-%d")


def test_sql_expression_string_literal():
    sql_expression = SQLExpression()
    sql_expression.compile("'10'")
    assert sql_expression.search({"fname": "john",  "mname": "george", "lname": "smith"}) == "10"


def test_sql_expression_list():
    sql_expression = SQLExpression()
    sql_expression.compile("fname || ' ' || mname || ' ' || lname")
    assert sql_expression.search_bulk([{"fname": "john",  "mname": "george", "lname": "smith"}]) == [
        "john george smith"]


def test_sql_expression_multiple_fields():
    sql_expression = SQLExpression()
    sql_expression.compile('{"fullname":"fname || \' \' || lname", "fname":"upper(fname)"}')
    assert sql_expression.search_bulk([{"fname": "john",  "mname": "george", "lname": "smith"}]) == [
        {"fullname": "john smith", "fname": "JOHN"}]


def test_sql_expression_sql_parse_error():
    sql_expression = SQLExpression()
    with pytest.raises(ValueError):
        sql_expression.compile("like 'A%'")


def test_sql_expression_sql_parse_error_malformed():
    sql_expression = SQLExpression()
    with pytest.raises(ValueError):
        sql_expression.compile("like 'A%")


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
         {"fname": "a", "id": 3}], tombstone=True) == [
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


def test_sql_nonexistent():
    sql_expression = SQLExpression()
    sql_expression.compile("a")
    assert sql_expression.search({"b": "c"}) == None


def test_sql_nonexistent_bulk():
    sql_expression = SQLExpression()
    sql_expression.compile("a")
    assert sql_expression.search_bulk([{"b": "c"}]) == [None]


def test_sql_nonexistent_filter():
    sql_expression = SQLExpression()
    sql_expression.compile("a='Y'")
    assert sql_expression.filter([{"a": "Y"}, {"b": "c"}]) == [{"a": "Y"}]


def test_sql_nonexistent_filter_ifnull():
    sql_expression = SQLExpression()
    sql_expression.compile("IFNULL(a,'Y')='Y'")
    assert sql_expression.filter([{"a": "Y"}, {"b": "c"}, {"a": "d"}]) == [{"a": "Y"}, {"b": "c"}]


def test_sql_concat_null():
    sql_expression = SQLExpression()
    sql_expression.compile("a||' '||b")
    assert sql_expression.search_bulk([{"a": "Y", "b": "Z"}, {"b": "c"}]) == ["Y Z", None]
