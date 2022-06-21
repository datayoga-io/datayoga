from datayoga.blocks import utils


def test_exec_sql():
    assert utils.exec_sql(
        utils.get_connection(),
        [("fname", "john"), ("mname", "george"), ("lname", "smith")],
        "fname || ' ' || mname || ' ' || lname") == "john george smith"
