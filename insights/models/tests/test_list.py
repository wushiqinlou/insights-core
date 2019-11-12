from insights.models import List


def test_islist():
    assert isinstance(List(), list)


def test_empty():
    empty = List()
    assert not bool(empty)
    assert len(empty) == 0


def test_simple_contains():
    simple = List([1, 2])
    assert bool(simple)
    assert len(simple) == 2

    assert 1 in simple
    assert 2 in simple
    assert 3 not in simple


def simple_index():
    simple = List([1, 2])
    assert simple[0] == 1
    assert simple[1] == 2


def test_simple_iteration():
    simple = List([1, 2])

    total = 0
    for i in simple:
        total += i

    assert total == 3


def test_equality():
    assert List([1, 2]) == List([1, 2])
    assert List([1, 2]) == [1, 2]
    assert [1, 2] == List([1, 2])
