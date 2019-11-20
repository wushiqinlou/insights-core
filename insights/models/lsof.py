from insights import parser
from insights.specs import Specs
from insights.models import Dict, List


def get_intervals(line):
    looking_for_start = True
    results = []
    t = 0
    for i, c in enumerate(line):
        if looking_for_start:
            if c.isspace():
                continue
            t = i
            looking_for_start = False
        else:
            if not c.isspace():
                continue
            results.append((t, i))
            looking_for_start = True
    if t and i:
        results.append((t, i + 1))
    return results


def intersect(a, b):
    return max((a[0], b[0])) < min((a[1], b[1]))


@parser(Specs.lsof)
def lsof(ctx):
    content = ctx.content
    top = content[0]
    header_intervals = get_intervals(top)
    headers = dict((top[l:r].lower(), (l, r)) for (l, r) in header_intervals)
    results = List()
    for line in content[1:]:
        one = []
        intervals = get_intervals(line)
        for i in intervals:
            val = line[slice(*i)]
            for key, h in headers.items():
                if intersect(i, h):
                    one.append((key, val))
                    break
        results.append(Dict(one, parent=results))
    return results
