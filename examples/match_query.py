from orientus.core.datatypes import OString
from orientus.core.domain import OVertex
from orientus.core.session import Graph


# with OrientUsDB('test', 'root', 'admin', debug=True) as db:
#     with Session(db) as session:
#         session.match() \
class Token(OVertex):
    text = OString(name='text', mandatory=True)
    new_text = OString(name='new_text', mandatory=True)


if __name__ == '__main__':
    Graph().match() \
        .vertex(class_=Token, alias='token') \
        .where((Token.text == 'the').or_(Token.text == 'THE')) \
        .outE('PreviousTokenEdge') \
        .vertex(class_=Token, alias='ngram') \
        .where(Token.depth == 1) \
        .when((Token.depth < 1).and_(Token.text == 'the')) \
        .return_result('ngram', ) \
        .group_by('token') \
        .order_by('token') \
        .skip(number=10) \
        .limit(number=100) \
        .done()

    # t1 = Token(text='first text')
    # t2 = Token()
    # print(Token.text == 'second text')
    # print(t1.text == 'second text')
