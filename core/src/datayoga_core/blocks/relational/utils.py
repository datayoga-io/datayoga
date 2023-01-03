from enum import Enum, unique


@unique
class DbType(Enum):
    MSSQL = "mssql"
    MYSQL = "mysql"
    PSQL = "postgresql"

    @classmethod
    def has_value(cls, value: str) -> bool:
        return value in cls._value2member_map_


DEFAULT_DRIVERS = {
    DbType.MYSQL.value: "mysql+pymysql",
    DbType.MSSQL.value: "mssql+pymssql",
    DbType.PSQL.value: "postgresql"
}
