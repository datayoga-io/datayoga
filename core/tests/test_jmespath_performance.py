import logging
import time

import pytest
from datayoga_core import expression


@pytest.mark.parametrize("batchsize,expected_ops_per_sec,fields", [(100,40000,20), (1000, 40000,20), (10000, 40000,20)])
def test_sql_benchmark(batchsize:int,expected_ops_per_sec:int,fields:int):
    """ A rough sanity benchmark to test the ballpark figures of the expression language

    Args:
        batchsize (int): size of batch
        expected_ops_per_sec (int): expected operations per sec
    """
    # suppress logging
    logging.getLogger("dy").disabled = True
    expression_text = "upper(field1)"
    expr = expression.compile(expression.Language.JMESPATH.value,expression_text)
    cycles = 200000
    start = time.time()
    batch = 1000

    # dummy record with 20 fields
    record = {f"field{i}":"abcdefghij" for i in range(fields)}
    for _ in range(cycles//batch):
        results = expr.search([record]*batch)

    end = time.time()

    logging.getLogger("dy").disabled = False
    actual_ops_per_sec = cycles/(end-start)
    logging.debug(f"ops per sec: {actual_ops_per_sec}")
    assert actual_ops_per_sec>expected_ops_per_sec
