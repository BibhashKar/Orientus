import inspect
from typing import List, Type

from pyorient import OrientRecord

from orientus.core.datatypes import RawType
from orientus.core.domain import ORecord
from orientus.tests.data import Token


def get_class_datatypes(class_type: Type[ORecord], exclude_rawtypes=True):
    """Returns Domain class variables only having Rawtype or its subclasses

    :param class_type: subclass type of ORecord
    :param exclude_rawtypes:
    :return: class variables of domain class
    """
    attributes = inspect.getmembers(class_type, lambda a: not (inspect.isroutine(a)))

    class_datatypes = [a for a in attributes if not (a[0].startswith('__') and a[0].endswith('__'))]
    if exclude_rawtypes:
        class_datatypes = [(name, v) for (name, v) in class_datatypes if
                           v.__class__ != RawType and issubclass(v.__class__, RawType)]

    return class_datatypes


def to_datatype_obj(class_type: Type[ORecord], records: List[OrientRecord]) -> List:
    """Converts OrientRecord list to Datatype object list

    :param class_type: subclass type of ORecord
    :param records: OrientRecord list
    :return: converted list
    """
    obj_list = []

    domain_variables = get_class_datatypes(class_type)
    column_name_vs_var_name = {datatype.name: name for (name, datatype) in domain_variables}

    print(column_name_vs_var_name)

    for record in records:
        """for this __init__(), Domain classes have to provide default params in constructor method"""
        instance = class_type()

        instance._version = record._version
        instance._rid = record._rid

        for key, value in record.oRecordData.items():
            if column_name_vs_var_name.get(key) is not None:
                setattr(instance, column_name_vs_var_name.get(key), value)

        obj_list.append(instance)

    return obj_list


if __name__ == '__main__':
    print(get_class_datatypes(Token, True))
