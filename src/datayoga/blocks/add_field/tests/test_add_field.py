from datayoga.blocks.add_field import Block


def test_add_field():
    block = Block()
    block.__init__({"field": "full_name",
                    "language": "jmespath",
                    "expression": '{ "fname": fname, "lname": lname} | join('' '', values(@))'})
    assert True
