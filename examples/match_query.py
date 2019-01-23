from orientus.core.datatypes import OString
from orientus.core.domain import OVertex, OEdge
from orientus.core.session import Graph


# with OrientUsDB('test', 'root', 'admin', debug=True) as db:
#     with Session(db) as session:
#         session.match() \
class Token(OVertex):
    ___vertex_name__ = 'Token'
    text = OString(name='text', mandatory=True)
    new_text = OString(name='new_text', mandatory=True)


class PreviousTokenEdge(OEdge):
    __edge_name__ = 'PreviousTokenEdge'
    pass


if __name__ == '__main__':
    Graph().match() \
        .vertex(vertex=Token, alias='token').where((Token.text == 'the') | (Token.text == 'THE')) \
        .outE(PreviousTokenEdge) \
        .vertex(vertex=Token, alias='ngram').where(Token.depth == 1).while_((Token.depth < 1) & (Token.text == 'the')) \
        .not_() \
        .vertex(vertex=Token, alias='tkn') \
        .outE(PreviousTokenEdge) \
        .vertex(vertex=Token, alias='tkn_') \
        .return_('$matches', ) \
        .group_by('token') \
        .order_by('token') \
        .skip(number=10) \
        .limit(number=100) \
        .done()

    # t1 = Token(text='first text')
    # t2 = Token()
    # print(Token.text == 'second text')
    # print(t1.text == 'second text')
