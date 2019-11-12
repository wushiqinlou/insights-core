import json
from insights import parser, Parser
from insights.specs import Specs
from insights.parsr.query import from_dict, Result


@parser(Specs.installed_rpms)
class Rpms(Parser):
    def parse_content(self, content):
        results = [from_dict(json.loads(l)) for l in content]
        self.doc = Result(children=results)
