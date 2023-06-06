import logging
import time

import pytest
from datayoga_core import expression


@pytest.mark.parametrize("batch_size, expected_ops_per_sec, fields",
                         [(100, 100000, 20),
                          (1000, 100000, 20),
                          (10000, 100000, 20)])
def test_jmespath_benchmark(batch_size:int, expected_ops_per_sec:int, fields:int):
    """A rough sanity benchmark to test the ballpark figures of the expression language

    Args:
        batchsize (int): size of batch
        expected_ops_per_sec (int): expected operations per sec
    """
    # suppress logging
    logging.getLogger("dy").disabled = True
    expression_text = "upper(field1)"
    expr = expression.compile(expression.Language.JMESPATH, expression_text)
    cycles = 200000
    start = time.time()
    field_value = "abcdefghij"

    # dummy record with the given fields
    record = {f"field{i}": field_value for i in range(fields)}
    for _ in range(cycles//batch_size):
        results = expr.search_bulk([record]*batch_size)
        assert len(results) == batch_size

    # perform the remainder. e.g. 10 cycles on 4 batch size, add 2 more
    if (cycles % batch_size > 0):
        results = expr.search_bulk([record]*(cycles%batch_size))

    end = time.time()

    # sanity test. check one result
    results = expr.search_bulk([record])
    assert results == [field_value.upper()]

    logging.getLogger("dy").disabled = False
    actual_ops_per_sec = cycles/(end-start)
    logging.debug(f"ops per sec: {actual_ops_per_sec}")
    assert actual_ops_per_sec>expected_ops_per_sec
