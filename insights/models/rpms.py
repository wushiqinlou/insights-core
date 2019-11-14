import json
from insights import parser
from insights.specs import Specs
from insights.models import from_dict, List


@parser(Specs.installed_rpms)
def rpms(ctx):
    results = List()
    results.extend([from_dict(json.loads(l), parent=results) for l in ctx.content])
    return results
