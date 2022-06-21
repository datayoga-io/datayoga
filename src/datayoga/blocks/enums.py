from enum import Enum, unique


@unique
class Language(Enum):
    JMESPATH = "jmespath"
    SQL = "sql"
