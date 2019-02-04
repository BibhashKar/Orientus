# TODO: implement select
"""
SELECT [ <Projections> ] [ FROM <Target> [ LET <Assignment>* ] ]
    [ WHERE <Condition>* ]
    [ GROUP BY <Field>* ]
    [ ORDER BY <Fields>* [ ASC|DESC ] * ]
    [ UNWIND <Field>* ]
    [ SKIP <SkipRecords> ]
    [ LIMIT <MaxRecords> ]
    [ FETCHPLAN <FetchPlan> ]
    [ TIMEOUT <Timeout> [ <STRATEGY> ]  RETURN or EXCEPTION
    [ LOCK default|record ]
    [ PARALLEL ]
    [ NOCACHE ]
"""

from orientus.core.query import Order, Query
from orientus.tests.data import Token

if __name__ == '__main__':
    # TODO: think about selecting columns from multiple classes

    # .select(Token.text, Token.new_text) \
    # .from_(Token) \

    Query(Token)\
        .where(Token.text == 'to') \
        .group_by(Token.text) \
        .order_by(Token.text, order=Order.DESC) \
        .unwind(Token.text) \
        .skip(number=3) \
        .limit(number=10) \
        .fetchplan() \
        .timeout(100) \
        .lock() \
        .parallel() \
        .cache() \
        ._done()
