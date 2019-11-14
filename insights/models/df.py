from insights import combiner, parser, Parser
from insights.specs import Specs
from insights.models import Dict, List


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
        results = List()
        num = len(DiskBase.header) - 1
        for line in content[1:]:
            parts = line.strip().split(None, num)
            parts = self.fix_types(parts)
            data = Dict(zip(DiskBase.header, parts), parent=results)
            data["capacity"] = data["capacity"] / 100
            results.append(data)

        self.doc = results


@parser(Specs.df__alP)
class Disk_alp(DiskBase):
    pass


@parser(Specs.df__al)
class Disk_al(DiskBase):
    pass


@combiner([Disk_alp, Disk_al])
def disk(*args):
    for a in args:
        if a is not None:
            return a.doc
