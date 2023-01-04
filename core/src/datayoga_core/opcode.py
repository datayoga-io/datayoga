from enum import Enum, unique


@unique
class OpCode(str, Enum):
    CREATE = "c"
    DELETE = "d"
    UPDATE = "u"
