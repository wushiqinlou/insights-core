from insights import parser, Parser
from insights.specs import Specs
from insights.parsr.query import from_dict, Result


@parser(Specs.meminfo)
class MemInfo(Parser):
    def parse_content(self, content):
        mem = {}
        for line in content:
            line = line.strip()
            k, v = line.split(":", 1)
            k = k.strip().rstrip(")").replace(" (", "_").lower()
            v = v.strip()
            v = int(v.split()[0]) * 1024 if v.endswith("kB") else int(v)
            mem[k] = v

        self.doc = Result(children=[from_dict(mem)])
