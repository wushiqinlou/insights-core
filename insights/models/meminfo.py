from insights import parser
from insights.specs import Specs
from insights.models import Dict


@parser(Specs.meminfo)
def memory(ctx):
    mem = Dict()
    for line in ctx.content:
        line = line.strip()
        k, v = line.split(":", 1)
        k = k.strip().rstrip(")").replace(" (", "_").lower()
        v = v.strip()
        v = int(v.split()[0]) * 1024 if v.endswith("kB") else int(v)
        mem[k] = v
    return mem
