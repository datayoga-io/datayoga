import json
import logging
import os

import pytest
from common import redis_utils
from common.utils import run_job

logger = logging.getLogger("dy")


@pytest.fixture(scope="module")
def prepare_db():
    # pseudo code
    redis_container = redis_utils.get_redis_oss_container(redis_utils.REDIS_PORT)
    redis_container.start()

    yield

    # cleanup
    redis_container.stop()


@pytest.mark.xfail
def test_redis_read_pending_messages(tmpdir, prepare_db):

    redis_client = redis_utils.get_redis_client("localhost", redis_utils.REDIS_PORT)
    redis_client.xadd("emp", {"message": json.dumps({"id": 1, "fname": "john", "lname": "doe"})})
    # malformed record (missing fname and lname properties)
    redis_client.xadd("emp", {"message": json.dumps({"id": 3})})
    redis_client.xadd("emp", {"message": json.dumps({"id": 2, "fname": "jane", "lname": "doe"})})

    output_file = tmpdir.join("test_redis_read_pending_messages.txt")

    # the runner should terminate with an error because of the rejected record
    with pytest.raises(ValueError):
        run_job("tests.redis.abort.redis_to_stdout", piped_to=output_file)

    # only the first record processed successfully
    result = json.loads(output_file.read())
    assert result.get("full_name") == "john doe"

    # start over and verify that we still fail as the rejected record is read from the pending messages
    with pytest.raises(ValueError):
        run_job("tests.redis.abort.redis_to_stdout", piped_to=output_file)

    # run the same job (with the same name so the same consumer group will be used) but with ignore error_handling
    run_job("tests.redis.ignore.redis_to_stdout", piped_to=output_file)

    # the last record was processed successfully
    result = json.loads(output_file.read())
    assert result.get("full_name") == "jane doe"

    os.remove(output_file)
