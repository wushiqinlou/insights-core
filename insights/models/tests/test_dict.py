from insights.models import Dict


def test_isdict():
    assert isinstance(Dict(), dict)


def test_empty():
    empty = Dict()
    assert not bool(empty)
    assert len(empty) == 0


def test_simple_contains():
    simple = Dict({"a": 1, "b": 2})
    assert bool(simple)
    assert len(simple) == 2

    assert "a" in simple
    assert "b" in simple
    assert "c" not in simple


def simple_index():
    simple = Dict({"a": 1, "b": 2})
    assert simple["a"] == 1
    assert simple["b"] == 2


def test_simple_iteration():
    simple = Dict({"a": 1, "b": 2})

    total = 0
    for k in simple:
        total += simple[k]

    assert total == 3


def test_attribute_access():
    simple = Dict({"a": 1, "b": 2})
    assert simple.a == 1
    assert simple.b == 2


def test_equality():
    assert Dict({"a": 1, "b": 2}) == Dict({"a": 1, "b": 2})
    assert Dict({"a": 1, "b": 2}) == {"a": 1, "b": 2}
    assert {"a": 1, "b": 2} == Dict({"a": 1, "b": 2})
