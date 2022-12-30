import logging

from common import db_utils
from common.utils import run_job

logger = logging.getLogger("dy")

SCHEMA = "hr"


def test_csv_to_pg():
    postgres_container = db_utils.get_postgres_container("postgres", "postgres", "postgres")
    postgres_container.start()

    engine = db_utils.get_engine(postgres_container)
    db_utils.create_schema(engine, SCHEMA)
    db_utils.create_emp_table(engine, SCHEMA)

    run_job("tests.csv_to_pg")

    total_employees = db_utils.select_one_row(engine, f"select count(*) as total from {SCHEMA}.emp")
    assert total_employees and total_employees["total"] == 3

    first_employee = db_utils.select_one_row(engine, f"select * from {SCHEMA}.emp where id = 1")
    assert first_employee is not None
    assert first_employee["id"] == 1
    assert first_employee["full_name"] == "John Doe"
    assert first_employee["country"] == "972 - ISRAEL"
    assert first_employee["gender"] == "M"

    second_employee = db_utils.select_one_row(engine, f"select * from {SCHEMA}.emp where id = 2")
    assert second_employee is not None
    assert second_employee["id"] == 2
    assert second_employee["full_name"] == "Jane Doe"
    assert second_employee["country"] == "972 - ISRAEL"
    assert second_employee["gender"] == "F"

    third_employee = db_utils.select_one_row(engine, f"select * from {SCHEMA}.emp where id = 3")
    assert third_employee is not None
    assert third_employee["id"] == 3
    assert third_employee["full_name"] == "Bill Adams"
    assert third_employee["country"] == "1 - USA"
    assert third_employee["gender"] == "M"

    postgres_container.stop()
