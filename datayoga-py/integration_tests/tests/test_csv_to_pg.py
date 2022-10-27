import logging

import common.pg as pg
from common.utils import run_job

logger = logging.getLogger("dy")


def test_csv_to_pg():
    postgres_container = pg.get_postgres_container()
    postgres_container.start()

    engine = pg.get_engine(postgres_container)
    pg.create_emp_table(engine, "hr")

    run_job("test_csv_to_pg.yaml")

    total_employees = pg.select_one_row(engine, "select count(*) as total from hr.emp")
    assert total_employees["total"] == 3

    first_employee = pg.select_one_row(engine, "select * from hr.emp where id = 1")

    assert first_employee["id"] == 1
    assert first_employee["full_name"] == "John Doe"
    assert first_employee["country"] == "972 - ISRAEL"
    assert first_employee["gender"] == "M"

    second_employee = pg.select_one_row(engine, "select * from hr.emp where id = 2")
    assert second_employee["id"] == 2
    assert second_employee["full_name"] == "Jane Doe"
    assert second_employee["country"] == "972 - ISRAEL"
    assert second_employee["gender"] == "F"

    third_employee = pg.select_one_row(engine, "select * from hr.emp where id = 3")
    assert third_employee["id"] == 3
    assert third_employee["full_name"] == "Bill Adams"
    assert third_employee["country"] == "1 - USA"
    assert third_employee["gender"] == "M"

    postgres_container.stop()
