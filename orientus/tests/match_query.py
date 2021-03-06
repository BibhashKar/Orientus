from typing import Mapping

from orientus.core.datatypes import OString
from orientus.core.db import OrientUsDB
from orientus.core.domain import OVertex, OEdge
from orientus.core.match import Graph
from orientus.core.session import Session


class Token(OVertex):
    ___vertex_name__ = 'Token'
    text = OString(name='text', mandatory=True)
    new_text = OString(name='new_text', mandatory=True)


class PreviousTokenEdge(OEdge):
    __edge_name__ = 'PreviousTokenEdge'
    pass


graph_ = Graph() \
    .vertex(vertex=Token, alias='token').where((Token.text == 'the') | (Token.text == 'THE')) \
    .outE(PreviousTokenEdge) \
    .vertex(vertex=Token, alias='ngram').where(Token.depth == 1).while_((Token.depth < 1) & (Token.text == 'the')) \
    .not_().vertex(vertex=Token, alias='token') \
    .outE(PreviousTokenEdge) \
    .vertex(vertex=Token, alias='ngram') \
    .return_('$matches', ) \
    .group_by('token') \
    .order_by('token') \
    .skip(number=10) \
    .limit(number=100)


def match_query_test():
    result = graph_.done() \
        .replace("\n", " ")

    expected_result = " ".join(["MATCH",
                                "{as: token, class: Token, where: ((text = 'the' OR text = 'THE'))}",
                                ".out(PreviousTokenEdge)",
                                "{as: ngram, class: Token, where: ($depth = 1), while: (($depth < 1 AND text = 'the'))}",
                                ", NOT",
                                "{as: token, class: Token}",
                                ".out(PreviousTokenEdge)",
                                "{as: ngram, class: Token}",
                                "RETURN $matches",
                                "GROUP BY token",
                                "ORDER BY token",
                                "SKIP 10",
                                "LIMIT 100", ])

    assert result == expected_result

def get_db_props() -> Mapping:
    return {'host': 'localhost',
            'port': 2424,
            'db_name': 'test',
            'username': 'root',
            'password': 'admin'}


if __name__ == '__main__':
    match_query_test()

    db_name = 'test'

    with OrientUsDB(get_db_props(), debug=True) as db:
        with Session(db) as session:
            session.match(graph_)
