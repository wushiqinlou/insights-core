from insights import parser
from insights.specs import Specs
from insights.models import Dict, List


@parser(Specs.cpuinfo)
def cpu(ctx):
    results = List()
    cpu = Dict(parent=results)
    for line in ctx.content:
        line = line.strip()
        if not line:
            if cpu:
                results.append(cpu)
                cpu = Dict(parent=results)
            continue
        key, value = line.split(":", 1)
        key = key.strip().replace(" ", "_").lower()
        value = value.strip()
        cpu[key] = value

    return results
