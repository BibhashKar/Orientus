class RawType:

    def __init__(self, min: int = None,
                 max: int = None,
                 mandatory: bool = False,
                 readonly: bool = False,
                 nullable: bool = False,
                 unique: bool = False,
                 regex: str = None):
        self.min = min
        self.max = max
        self.mandatory = mandatory
        self.readonly = readonly
        self.nullable = nullable
        self.unique = unique
        self.regex = regex

    def __eq__(self, other):
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
    pass


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
