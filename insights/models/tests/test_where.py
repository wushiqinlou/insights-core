from insights.models import Dict, List
from insights.models import child_query as q
from insights.parsr.query import startswith

api = Dict({
    "kind": "KubeAPIServer",
    "status": Dict({
        "conditions": List([
            Dict({
                "type": "Ready",
                "status": "True"
            }),
            Dict({
                "type": "Failing",
                "status": "False"
            }),
            Dict({
                "type": "Initializing",
                "status": "False"
            }),
        ])
    })
})

sched = Dict({
    "kind": "KubeScheduler",
    "status": Dict({
        "conditions": List([
            Dict({
                "type": "Ready",
                "status": "True"
            }),
            Dict({
                "type": "Failing",
                "status": "False"
            }),
            Dict({
                "type": "Initializing",
                "status": "False"
            }),
        ])
    })
})


def test_where_name():
    conf = List([api, sched])
    assert len(conf.where("kind")) == 2
    assert len(conf.where("status")) == 2
    assert len(conf.status.where("conditions")) == 2
    assert len(conf.status.conditions.where("type")) == 6


def test_where_name_value():
    conf = List([api, sched])
    assert len(conf.where("kind", "KubeAPIServer")) == 1
    assert len(conf.where("status")) == 2
    assert len(conf.status.where("conditions")) == 2
    assert len(conf.status.conditions.where("type")) == 6


def test_where_name_expr():
    conf = List([api, sched])
    assert len(conf.where(startswith("ki"), "KubeAPIServer")) == 1


def test_where_value_expr():
    conf = List([api, sched])
    assert len(conf.where("kind", startswith("KubeAPI"))) == 1


def test_where_both_expr():
    conf = List([api, sched])
    assert len(conf.where(startswith("kin"), startswith("KubeAPI"))) == 1


def test_where_lambda():
    conf = List([api, sched])
    res = conf.where(lambda n: n.kind == "KubeAPIServer")
    assert len(res) == 1, res


def test_where_child_query():
    conf = List([api, sched])
    res = conf.where(q("kind", "KubeAPIServer"))
    assert len(res) == 1, res


def test_where_compound_child_query():
    conf = List([api, sched])
    p = q("kind", "KubeAPIServer") | q("kind", "KubeScheduler")
    res = conf.where(p)
    assert len(res) == 2, res

    conf = List([api, sched])
    p = q("type", "Ready") & q("status", "True")
    res = conf.status.conditions.where(p)
    assert len(res) == 2, res


def test_where_child_query_value_predicate():
    conf = List([api, sched])
    p = q("kind", startswith("Kube"))
    res = conf.where(p)
    assert len(res) == 2, res


def test_where_child_query_name_predicate():
    conf = List([api, sched])
    p = q(startswith("ki"), "KubeAPIServer")
    res = conf.where(p)
    assert len(res) == 1, res


def test_where_child_query_both_predicate():
    conf = List([api, sched])
    p = q(startswith("ki"), startswith("Kube"))
    res = conf.where(p)
    assert len(res) == 2, res
