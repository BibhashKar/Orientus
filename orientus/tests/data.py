from orientus.core.datatypes import OString
from orientus.core.domain import OVertex, OEdge


class Token(OVertex):
    ___vertex_name__ = 'Token'
    text = OString(name='text', mandatory=True)
    new_text = OString(name='new_text', mandatory=True)


class PreviousTokenEdge(OEdge):
    __edge_name__ = 'PreviousTokenEdge'
    pass
