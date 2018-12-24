from core import OGraph, OElement, OVertex, OEdge


class DocClass(OElement):
    pass


class VertexClass(OVertex):
    pass


class EdgeClass(OEdge):
    pass


with OGraph() as db:
    d = DocClass()
    d.set_property('foo', 'bar')
    d.save()

    v1 = VertexClass()
    v1.set_property('foo', 'one')
    v1.save()

    v2 = VertexClass()
    v2.set_property('foo', 'two')
    v2.save()

    e1 = EdgeClass()
    v1.add_edge(v2, e1).save()
