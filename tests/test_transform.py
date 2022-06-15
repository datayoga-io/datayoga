

import logging
import os
from os import path

from datayoga import dy
from datayoga.utils import read_yaml

logger = logging.getLogger(__name__)


def test_transform():
    job = read_yaml(path.join(os.path.dirname(os.path.realpath(__file__)), "test.yaml"))
    logger.debug(f"job: {job}")
    assert dy.transform(job) == "hello"
