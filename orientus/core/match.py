from typing import Type

from orientus.core.datatypes import Clause
from orientus.core.domain import OVertex, OEdge


def prop(*args):
    pass


class GraphFunction:

    def __init__(self, edge_class=None):
        pass


class Graph(GraphFunction):

    def __init__(self):
        super().__init__()

        self.__sql = []

        self.__vertex_on = False
        self.__vertex_dict = {}

    def vertex(self, vertex: Type[OVertex], alias: str) -> 'Graph':
        self.__vertex_on = True
        self.__vertex_dict = {'class': vertex.___vertex_name__, 'as': alias}
        return self

    def where(self, clause: Clause) -> 'Graph':
        if self.__vertex_on:
            self.__vertex_dict['where'] = str(clause)
        return self

    def while_(self, clause: Clause) -> 'Graph':
        if self.__vertex_on:
            self.__vertex_dict['while'] = str(clause)
        return self

    def outE(self, edge: Type[OEdge]) -> 'Graph':
        self.__close_vertex()
        self.__sql.append(".%s(%s)" % ("out", edge.__edge_name__))
        return self

    def inE(self, edge: Type[OEdge]) -> 'Graph':
        self.__close_vertex()
        self.__sql.append(".%s(%s)" % ('in', edge.__edge_name__))
        return self

    def bothE(self, edge: Type[OEdge]) -> 'Graph':
        self.__close_vertex()
        self.__sql.append(".%s(%s)" % ('both', edge))
        return self

    def not_(self) -> 'Graph':
        self.__close_vertex()
        self.__sql.append(", NOT")
        return self

    def return_(self, clause: str, distinct=False, alias=None) -> 'Graph':
        self.__close_vertex()

        return_str = "RETURN"

        if distinct:
            return_str += " DISTINCT"

        return_str += " %s" % (clause)

        if alias:
            return_str += " %s" % (alias)

        self.__sql.append(return_str)

        return self

    def group_by(self, clause: str) -> 'Graph':
        self.__close_vertex()
        if clause:
            self.__sql.append("GROUP BY %s" % (clause))
        return self

    def order_by(self, clause: str) -> 'Graph':
        self.__close_vertex()
        if clause:
            self.__sql.append("ORDER BY %s" % (clause))
        return self

    def skip(self, number: int) -> 'Graph':
        self.__close_vertex()
        self.__sql.append("SKIP %s" % (number))
        return self

    def limit(self, number: int) -> 'Graph':
        self.__close_vertex()
        self.__sql.append("LIMIT %s" % (number))
        return self

    def done(self) -> str:
        self.__close_vertex()
        return "MATCH\n" + "\n".join(self.__sql)

    def __close_vertex(self):
        if self.__vertex_on:

            self.__vertex_on = False

            class_str = "as: %s" % (self.__vertex_dict['as'])

            if self.__vertex_dict.get('class') != None:
                class_str += ", class: %s" % (self.__vertex_dict['class'])

            if self.__vertex_dict.get('where') != None:
                class_str += ", where: (%s)" % (self.__vertex_dict.get('where'))

            if self.__vertex_dict.get('while') != None:
                class_str += ", while: (%s)" % (self.__vertex_dict.get('while'))

            self.__sql.append("{%s}" % (class_str))

            self.__vertex_dict = {}