from yapp.core import AttrDict


def test_attr_dict():
    d1 = AttrDict()
    d2 = AttrDict({"a": 2})
    d3 = AttrDict({"a": 23, "b": 12})
    d4 = AttrDict({"a": 23, "b": 12})
    assert d1 != d2
    assert d3 == d4
    assert len(d1) == 0
    assert len(d4) == 2

    assert d3.a == d3["a"]
    assert d3.a == 23

    d3.b = 100  # type: ignore
    assert d3.b == 100


def test_recursive_convert():
    d1 = AttrDict(
        {
            "a": {"a1": [12, 13, 14]},
            "b": [
                {"b1": {"b11": 11, "b12": 12}},
                {"xx": [1, 2, 3, 4, 5]},
                {"xx": {"c": {}}},
            ],
        }
    )

    assert type(d1.a) == AttrDict
    assert d1.b[0].b1.b11 == 11  # type: ignore
