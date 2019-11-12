from functools import reduce
from itertools import chain

from insights.parsr.query.boolean import All, Any, Boolean, Not, Predicate, pred
from insights.parsr.query import ge, gt, le, lt


_NONE = object()


class ChildQuery(Boolean):
    def __and__(self, other):
        return ChildAll(self, other)

    def __or__(self, other):
        return ChildAny(self, other)

    def __invert__(self):
        return ChildNot(self)


class SimpleQuery(ChildQuery):
    def __init__(self, name, value=_NONE):
        self.expr = self.create_expr(name, value)

    def create_expr(self, name, value):
        return self.desugar(name, value)

    def both_predicates(self, n, a):
        def both(d):
            return any(n.test(k) and a.test(d[k]) for k in d)
        return pred(both)

    def name_predicate(self, n, a):
        def name(d):
            if a is not _NONE:
                return any(n.test(k) and d[k] == a for k in d)
            else:
                return any(n.test(k) for k in d)
        return pred(name)

    def attr_predicate(self, n, a):
        def attr(d):
            if n is None:
                return any(a.test(v) for v in d.values())
            return a.test(d[n])
        return pred(attr)

    def neither_predicate(self, n, a):
        def neither(d):
            if n is None and a is _NONE:
                return True
            elif a is _NONE:
                return n in d
            elif n is None:
                return a in set(d.values())
            return d[n] == a
        return pred(neither)

    def desugar(self, name, attr=_NONE):
        if isinstance(name, Predicate) and isinstance(attr, Predicate):
            return self.both_predicates(name, attr)
        elif isinstance(name, Predicate):
            return self.name_predicate(name, attr)
        elif isinstance(attr, Predicate):
            return self.attr_predicate(name, attr)
        return self.neither_predicate(name, attr)

    def test(self, n):
        return self.expr.test(n)

    def __repr__(self):
        return f"q({self.expr})"


class ColQuery(SimpleQuery):
    def __init__(self, name):
        self.name = name

    def __gt__(self, other):
        self.expr = self.create_expr(self.name, gt(other))
        return self

    def __ge__(self, other):
        self.expr = self.create_expr(self.name, ge(other))
        return self

    def __lt__(self, other):
        self.expr = self.create_expr(self.name, lt(other))
        return self

    def __le__(self, other):
        self.expr = self.create_expr(self.name, le(other))
        return self

    def __eq__(self, other):
        self.expr = self.create_expr(self.name, other)
        return self

    def __ne__(self, other):
        self.expr = ~self.create_expr(self.name, other)
        return self


class ChildAll(ChildQuery, All):
    pass


class ChildAny(ChildQuery, Any):
    pass


class ChildNot(ChildQuery, Not):
    pass


child_query = SimpleQuery
make_child_query = SimpleQuery


class Base(object):
    def __init__(self, data=None, parent=None):
        # dicts and lists aren't hashable.  I know I mean instance identity, so
        # I use the original id as a stand-in.
        self._id = id(self)
        self.parent = parent
        if data is not None:
            super().__init__(data)
        else:
            super().__init__()

    @property
    def root(self):
        cur = self.parent
        parent = cur
        while cur is not None:
            parent = cur
            cur = cur.parent
        return parent


class Dict(Base, dict):
    def __getattr__(self, name):
        return self[name]

    def __dir__(self):
        return list(self.keys()) + object.__dir__(self)

    def both_predicates(self, n, a):
        def both(i):
            k, v = i
            return n.test(k) and a.test(v)
        return pred(both)

    def name_predicate(self, n, a):
        def name(i):
            k, v = i
            if a is not _NONE:
                return n.test(k) and v == a
            else:
                return n.test(k)
        return pred(name)

    def attr_predicate(self, n, a):
        def attr(i):
            k, v = i
            if n is None:
                return a.test(v)
            return k == n and a.test(v)
        return pred(attr)

    def neither_predicate(self, n, a):
        def neither(i):
            k, v = i
            if n is None and a is _NONE:
                return True
            elif a is _NONE:
                return k == n
            elif n is None:
                return v == a
            return k == n and v == a
        return pred(neither)

    def desugar(self, name, attr=_NONE):
        if isinstance(name, Predicate) and isinstance(attr, Predicate):
            return self.both_predicates(name, attr)
        elif isinstance(name, Predicate):
            return self.name_predicate(name, attr)
        elif isinstance(attr, Predicate):
            return self.attr_predicate(name, attr)
        return self.neither_predicate(name, attr)

    def __getitem__(self, query):
        if isinstance(query, (tuple, Predicate)):
            query = self.desugar(*query) if isinstance(query, tuple) else self.desugar(query)
            return Dict([i for i in self.items() if query.test(i)], parent=self)

        res = super().__getitem__(query)
        if isinstance(res, dict):
            return Dict(res, parent=self)
        elif isinstance(res, list):
            return List(res, parent=self)
        return res

    def upto(self, query):
        cur = self.parent
        while cur is not None:
            if query.test(cur):
                return cur
            cur = cur.parent


class List(Base, list):
    def __getattr__(self, name):
        return self[name]

    def _query(self, q):
        res = List()
        for i in self:
            try:
                v = i[q]
                if v:
                    if isinstance(v, list):
                        res.extend(v)
                    else:
                        res.append(v)
            except Exception:
                pass
        return res

    def __getitem__(self, query):
        if not isinstance(query, (int, slice)):
            return self._query(query)
        return super().__getitem__(query)

    def where(self, name_query, value_query=_NONE):
        if isinstance(name_query, ChildQuery):
            return List(i for i in self if name_query.test(i))

        if callable(name_query) and not isinstance(name_query, Predicate):
            query = pred(name_query)
            return List(i for i in self if query.test(i))

        return self[name_query, value_query].parents

    def __contains__(self, key):
        for i in self:
            try:
                if key == i or key in i:
                    return True
            except:
                pass
        return False

    def __dir__(self):
        return self.keys() + object.__dir__(self)

    def upto(self, name):
        if isinstance(name, Predicate):
            query = name
        elif isinstance(name, str):
            query = pred(lambda n: name in n.parent if n.parent else False)
        else:
            query = pred(name)

        roots = List()
        seen = set()
        for c in self:
            root = c.upto(query)
            if root is not None and root._id not in seen:
                roots.append(root)
                seen.add(root._id)
        return roots

    def keys(self):
        try:
            return sorted(reduce(set.union, [d.keys() for d in self], set()))
        except:
            return []

    @property
    def roots(self):
        seen = set()
        results = List()
        for d in self:
            root = d.root
            if root is not None and root._id not in seen:
                results.append(root)
                seen.add(root._id)
        return results

    @property
    def parents(self):
        seen = set()
        results = List()
        for d in self:
            parent = d.parent
            if parent and parent._id not in seen:
                results.append(parent)
                seen.add(parent._id)
        return results


def from_list(l, parent=None):
    result = List(parent=parent)
    if l:
        if isinstance(l[0], dict):
            result.extend(from_dict(i, parent=result) for i in l)
        else:
            result.extend(l)
    return result


def from_dict(d, parent=None):
    result = Dict(parent=parent)
    for k, v in d.items():
        if isinstance(v, dict):
            result[k] = from_dict(v, parent=result)
        elif isinstance(v, list):
            result[k] = from_list(v, parent=result)
        else:
            result[k] = v
    return result
