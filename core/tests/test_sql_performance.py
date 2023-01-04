import logging
import time

import pytest
from datayoga_core import expression


@pytest.mark.parametrize("batch_size, expected_ops_per_sec, fields",
                         [(100, 40000, 20),
                          (1000, 40000, 20),
                          (10000, 40000, 20)])
def test_sql_benchmark(batch_size: int, expected_ops_per_sec: int, fields: int):
    """ A rough sanity benchmark to test the ballpark figures of the expression language

    Args:
        batch_size (int): size of batch
        expected_ops_per_sec (int): expected operations per sec
        fields (int): number of fields
    """
    # suppress logging
    logging.getLogger("dy").disabled = True
    expression_text = "replace(replace(replace(replace(field1, 'a', 'A'), 'b', 'BB'), 'c', 'C'), 'dd', 'D')"
    expr = expression.compile(expression.Language.SQL, expression_text)
    cycles = 200000
    start = time.time()

    # dummy record with the given fields
    record = {f"field{i}": "01234567890" for i in range(fields)}
    for _ in range(cycles//batch_size):
        results = expr.search_bulk([record]*batch_size)

    if (cycles % batch_size > 0):
        results = expr.search_bulk([record]*(cycles % batch_size))

    end = time.time()

    logging.getLogger("dy").disabled = False
    actual_ops_per_sec = cycles/(end-start)
    logging.debug(f"ops per sec: {actual_ops_per_sec}")
    assert actual_ops_per_sec > expected_ops_per_sec


@pytest.mark.parametrize("batch_size, expected_ops_per_sec", [(100, 40000), (1000, 40000), (10000, 40000)])
def test_sql_benchmark_nested(batch_size: int, expected_ops_per_sec: int):
    """ A rough sanity benchmark to test the ballpark figures of the expression language

    Args:
        batch_size (int): size of batch
        expected_ops_per_sec (int): expected operations per sec
    """
    # suppress logging
    logging.getLogger("dy").disabled = True
    expression_text = "upper(`a.b.c.d`)"
    expr = expression.compile(expression.Language.SQL, expression_text)
    cycles = 200000
    start = time.time()

    # dummy record with nested fields
    record = {
        "a": {
            "x": "x"*10,
            "b": {
                "x": "x"*10,
                "c": {
                    "x": "x"*10,
                    "d": "d"*10
                }
            }
        }
    }
    for _ in range(cycles//batch_size):
        results = expr.search_bulk([record]*batch_size)

    # perform the remainder. e.g. 10 cycles on 4 batch size, add 2 more
    if (cycles % batch_size > 0):
        results = expr.search_bulk([record]*(cycles % batch_size))
    end = time.time()

    # sanity test. check one result
    results = expr.search_bulk([record])
    assert results == ["D"*10]

    logging.getLogger("dy").disabled = False
    actual_ops_per_sec = cycles/(end-start)
    logging.debug(f"ops per sec: {actual_ops_per_sec}")
    assert actual_ops_per_sec > expected_ops_per_sec
