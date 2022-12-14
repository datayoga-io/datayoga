from enum import Enum, unique


@unique
class OpCode(Enum):
    CREATE = "c"
    DELETE = "d"
    UPDATE = "u"
