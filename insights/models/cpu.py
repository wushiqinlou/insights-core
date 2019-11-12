from insights import parser, Parser
from insights.specs import Specs
from insights.models import Dict, List


@parser(Specs.cpuinfo)
class CPU(Parser):
    def parse_content(self, content):
        results = List()
        cpu = Dict()
        for line in content:
            line = line.strip()
            if not line:
                if cpu:
                    results.append(cpu)
                    cpu = Dict()
                continue
            key, value = line.split(":", 1)
            key = key.strip().replace(" ", "_").lower()
            value = value.strip()
            cpu[key] = value

        self.data = results

    def where(self, pred):
        return self.data.where(pred)
