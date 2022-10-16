from datayoga.blocks.explode.block import Block


def test_explode():
    block = Block(
        {
            "field": "features",
            "target_field": "feature",
            "delimiter": ";"
        }
    )
    assert block.run([{"product": "BMW 316i", "features": "fast;black;midsize;classD"}]) == [
        {"product": "BMW 316i", "features": "fast;black;midsize;classD", "feature": "fast"},
        {"product": "BMW 316i", "features": "fast;black;midsize;classD", "feature": "black"},
        {"product": "BMW 316i", "features": "fast;black;midsize;classD", "feature": "midsize"},
        {"product": "BMW 316i", "features": "fast;black;midsize;classD", "feature": "classD"}
    ]
