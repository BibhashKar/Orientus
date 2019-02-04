import inspect

from orientus.core.datatypes import RawType
from orientus.tests.data import Token


def domain_variables(clz, exclude_rawtypes=True):
    attributes = inspect.getmembers(clz, lambda a: not (inspect.isroutine(a)))
    variables = [a for a in attributes if not (a[0].startswith('__') and a[0].endswith('__'))]
    if exclude_rawtypes:
        variables = [(name, v) for (name, v) in variables if v.__class__ != RawType and issubclass(v.__class__, RawType)]
    return variables


if __name__ == '__main__':
    print(domain_variables(Token, True))
