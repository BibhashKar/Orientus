from core.db import OrientUs
from core.domain import OGraph, OVertex, OEdge, orientus_conn_stack


class Animal(OVertex):
    def __init__(self, kind, name):
        self.kind = kind
        self.name = name


class FriendShip(OEdge):
    pass


def sample():
    with OGraph() as db:
        v1 = Animal('Deer', 'Chapila')
        v1.save()

        v2 = Animal('Rabbit', 'Jaggu')
        v2.save()

        e1 = FriendShip()
        v1.add_edge(v2, e1).save()


if __name__ == '__main__':
    client = OrientUs('localhost', 2424)
    client.connect('root', 'admin')

    orientus_conn_stack.orient_client = client

    v1 = Animal('Deer', 'Chapila')
    v1.save()

    # client.db_open('knowledge', 'root', 'admin')

    # res = client.command('create class Animal extends V')
    # pprint(res)
    # client.command("insert into Food set name = 'pea', color = 'green'")
    # pprint(client.query('select from Sentence'))

    client.close()
