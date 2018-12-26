from enum import Enum
from typing import Any, List, Optional
import threading

orientus_conn_stack = threading.local()


class ORID:
    PREFIX = '#'
    SEPARATOR = ':'

    def __init__(self, rid):
        self.rid = rid

    def get_cluster(self) -> int:
        pass

    def get_cluster_position(self) -> int:
        pass


class ORecord:
    def save(self):
        print('in save() method')
        client = orientus_conn_stack.orient_client
        print(client)
        # get_database().save(self)

    def get_identity(self) -> ORID:
        pass

    def delete(self):
        pass


class OElement(ORecord):

    def get_property_names(self) -> List[str]:
        pass

    def is_vertex(self) -> bool:
        pass

    def is_edge(self) -> bool:
        pass

    def as_vertex(self) -> Optional['OVertex']:
        pass

    def as_edge(self) -> Optional['OEdge']:
        pass


class OBlob(ORecord):
    def from_input(self, input):
        pass

    def to_output(self, output):
        pass


class ODirection(Enum):
    IN = 'IN'
    OUT = 'OUT'
    BOTH = 'BOTH'


class OVertex(OElement):
    def add_edge(self, to: 'OVertex', edge_class: 'OEdge'):
        pass

    def get_edges(self, direction: ODirection) -> List['OEdge']:
        pass

    def get_vertices(self, direction: ODirection) -> List['OVertex']:
        pass


class OEdge(OElement):
    def get_from(self) -> OVertex:
        pass

    def get_to(self) -> OVertex:
        pass


class OGraph:
    pass
