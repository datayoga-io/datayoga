from enum import Enum, unique


@unique
class OpCode(Enum):
    CREATE = "c"
    DELETE = "d"
    UPDATE = "u"


OPCODES = [opcode.value for opcode in OpCode]
