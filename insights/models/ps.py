from insights import parser
from insights.models import Dict, List
from insights.specs import Specs


@parser(Specs.ps_auxww)
def ps(ctx):
    results = List()
    header = ["user", "pid", "cpu", "mem", "vsz", "rss", "tty", "stat", "start", "time", "command"]
    hl = len(header) - 1
    for line in ctx.content[1:]:
        parts = [l.strip() for l in line.split(None, hl)]
        data = Dict(zip(header, parts), parent=results)
        data["pid"] = int(data["pid"])
        data["cpu"] = float(data["cpu"])
        data["mem"] = float(data["mem"])
        data["vsz"] = float(data["vsz"])
        data["rss"] = float(data["rss"])
        results.append(data)
    return results
