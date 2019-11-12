from insights.models import Dict, List

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


def test_dict_chain():
    assert len(api.status.conditions) == 3

    types = api.status.conditions.type
    assert types == ["Ready", "Failing", "Initializing"]


def test_list_chain():
    conf = List([api, sched])
    assert len(conf.status) == 2
    assert len(conf.status.conditions) == 6, conf.status.conditions

    types = conf.status.conditions.type
    assert types == ["Ready", "Failing", "Initializing"] * 2
