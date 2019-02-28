class Clause:
    def __init__(self, expr: str):
        self.expression = expr

    def __repr__(self):
        return self.expression

    def __and__(self, other):
        return Clause("(%s AND %s)" % (self.expression, other.expression))

    def __or__(self, other):
        return Clause("(%s OR %s)" % (self.expression, other.expression))


class RawType:

    def __init__(self, name: str,
                 min: int = None,
                 max: int = None,
                 mandatory: bool = False,
                 readonly: bool = False,
                 nullable: bool = False,
                 unique: bool = False,
                 regex: str = None):
        self.name = name
        self.min = min
        self.max = max
        self.mandatory = mandatory
        self.readonly = readonly
        self.nullable = nullable
        self.unique = unique
        self.regex = regex

    def __eq__(self, other):
        if other is None:
            return Clause("%s IS %s" % (self.name, self.__get_other_value(other)))

        return Clause("%s = %s" % (self.name, self.__get_other_value(other)))

    def __ne__(self, other):
        if other is None:
            return Clause("%s IS NOT %s" % (self.name, self.__get_other_value(other)))

        return Clause("%s != %s" % (self.name, self.__get_other_value(other)))

    def __lt__(self, other):
        return Clause("%s < %s" % (self.name, self.__get_other_value(other)))

    def __le__(self, other):
        return Clause("%s <= %s" % (self.name, self.__get_other_value(other)))

    def __gt__(self, other):
        return Clause("%s > %s" % (self.name, self.__get_other_value(other)))

    def __ge__(self, other):
        return Clause("%s >= %s" % (self.name, self.__get_other_value(other)))

    def __get_other_value(self, other):
        if other is None:
            return 'NULL'
        if isinstance(other, str):
            return "'%s'" % (other)
        if isinstance(other, RawType):
            return other.name
        else:
            return other


class OPrimaryKey(RawType):
    pass


class OBoolean(RawType):
    pass


class OInteger(RawType):
    pass


class OShort(RawType):
    pass


class OLong(RawType):
    pass


class OFloat(RawType):
    pass


class ODouble(RawType):
    pass


class ODatetime(RawType):
    pass


class OString(RawType):

    def trim(self):
        return RawType('%s.%s' % (self.name, 'trim()'))


#     TODO: implement other string methods of orientdb


class OBinary(RawType):
    pass


class OEmbedded(RawType):
    pass


class OEmbeddedList(OEmbedded):
    pass


class OEmbeddedSet(OEmbedded):
    pass


class OEmbeddedMap(OEmbedded):
    pass


class OAbstractLink(RawType):
    pass


class OLink(OAbstractLink):
    pass


class OLinkList(OAbstractLink):
    pass


class OLinkSet(OAbstractLink):
    pass


class OLinkMap(OAbstractLink):
    pass


class OByte(RawType):
    pass


class OTransient(RawType):
    pass


class ODate(RawType):
    pass


class OCustom(RawType):
    pass


class ODecimal(RawType):
    pass


class OLinkBag(RawType):
    pass


class OAny(RawType):
    pass
