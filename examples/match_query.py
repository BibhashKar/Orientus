from orientus.core.db import OrientUsDB
from orientus.core.session import Session, prop, Graph

# with OrientUsDB('test', 'root', 'admin', debug=True) as db:
#     with Session(db) as session:
#         session.match() \
if __name__ == '__main__':
    Graph().match() \
        .vertex(class_type='Token', alias='token').where("text = 'the'") \
        .outE('PreviousTokenEdge') \
        .vertex(alias='ngram').where("$depth = 1").when("$depth < 1 and text = 'the'") \
        .return_result('ngram',) \
        .group_by('token') \
        .order_by('token') \
        .skip(number=10) \
        .limit(number=100) \
        .done()
