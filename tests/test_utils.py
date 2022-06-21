from datayoga.blocks import utils


def test_sql_expression():
    assert utils.exec_sql(
        utils.get_connection(),
        [("fname", "john"), ("mname", "george"), ("lname", "smith")],
        "fname || ' ' || mname || ' ' || lname") == "john george smith"
