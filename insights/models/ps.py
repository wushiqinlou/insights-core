from insights import parser, Parser
from insights.models import Dict, List
from insights.specs import Specs


@parser(Specs.ps_auxww)
class PS(Parser):
    header = ["user", "pid", "cpu", "mem", "vsz", "rss", "tty", "stat", "start", "time", "command"]

    def parse_content(self, content):
        results = List()
        l = len(PS.header) - 1
        for line in content[1:]:
            parts = [l.strip() for l in line.split(None, l)]
            data = Dict(zip(PS.header, parts))
            data["pid"] = int(data["pid"])
            data["cpu"] = float(data["cpu"])
            data["mem"] = float(data["mem"])
            data["vsz"] = float(data["vsz"])
            data["rss"] = float(data["rss"])
            results.append(data)
        self.doc = results
