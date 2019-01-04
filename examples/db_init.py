from pyorient import OrientRecord

from orientus.core.db import OrientUsDB, OrientUs
from orientus.core.domain import OVertex, OEdge, ORecord
from orientus.core.session import BatchSession


class Animal(OVertex):
    def __init__(self, kind, name):
        super().__init__()
        self.kind = kind
        self.name = name


class File(ORecord):
    def __init__(self, filename, size):
        super().__init__()

        self.filename = filename
        self.size = size


class Person(OVertex):
    def __init__(self, name):
        super().__init__()
        self.name = name


class Related(OEdge):
    def __init__(self):
        super().__init__()
        self.relation_type = 'love'


class FriendShip(OEdge):
    pass


def to_File(data: OrientRecord):
    values = data.__dict__['_OrientRecord__o_storage']
    f = File(values['filename'], values['size'])
    f._rid = data._rid
    f._version = data._version
    return f


class TestDoc(OVertex):
    def __init__(self, text):
        super().__init__()
        self.text = text


class TestSentence(OVertex):
    def __init__(self, text):
        super().__init__()
        self.text = text


class TestSentenceToDoc(OEdge):

    def __init__(self, frm_vertex, to_vertex):
        super().__init__(frm_vertex, to_vertex)
        # self.index = None


class TokenTest(OVertex):
    def __init__(self, text):
        super().__init__()
        # self.index = index
        self.text = text


def test_recreate_db():
    db_name = 'test'

    client = OrientUs()
    client.connect('root', 'admin')
    client.recreate_db(db_name)


if __name__ == '__main__':
    db_name = 'test'

    with OrientUsDB(db_name, 'root', 'admin', debug=True) as db:
        with BatchSession(db) as session:
            session.start_batch()

            var = "''"
            # var = "J'H"
            t = TokenTest(var.replace("'", "\\'"))
            session.save(t)

            # doc = TestDoc('. "I am Kelvin Clan". I am the brand. I am the brand. I am fashion.')
            # session.save(doc)
            #
            # for index, sent_text in enumerate(doc.text.split('.')):
            #     sent = TestSentence(sent_text)
            #     session.save(sent)
            #
            #     sent_doc_edge = TestSentenceToDoc(sent, doc)
            #     sent_doc_edge.index = index
            # session.save(sent_doc_edge)

            session.end_batch()
