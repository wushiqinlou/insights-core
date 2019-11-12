from insights import combiner, parser, Parser
from insights.specs import Specs
from insights.parsr.query import Dict, List


class DiskBase(Parser):
    header = ["filesystem", "blocks", "used", "available", "capacity", "mount"]

    def fix_types(self, parts):
        results = []
        for p in parts:
            if p == "-":
                results.append(0.0)
            elif p[0].isdigit():
                results.append(float(p.rstrip("%")))
            else:
                results.append(p)
        return results

    def parse_content(self, content):
        results = []
        num = len(DiskBase.header) - 1
        for line in content[1:]:
            parts = line.strip().split(None, num)
            parts = self.fix_types(parts)
            data = Dict(zip(DiskBase.header, parts))
            data["capacity"] = data["capacity"] / 100
            results.append(data)

        self.doc = List(children=results)


@parser(Specs.df__alP)
class Disk_alp(DiskBase):
    pass


@parser(Specs.df__al)
class Disk_al(DiskBase):
    pass


@combiner([Disk_alp, Disk_al])
def Disk(*args):
    return [a for a in args if a][0]
