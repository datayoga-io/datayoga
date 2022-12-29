from abc import ABCMeta, abstractmethod
from enum import Enum, unique
from typing import Any, Dict, List

from sqlalchemy import Table
from sqlalchemy.engine import CursorResult, Engine
from sqlalchemy.sql.expression import ColumnCollection


@unique
class DbType(Enum):
    MYSQL = "mysql"
    PSQL = "postgresql"

    @classmethod
    def has_value(cls, value: str) -> bool:
        return value in cls._value2member_map_


class Connector(metaclass=ABCMeta):
    def __init__(self, engine: Engine):
        self.connection = engine.connect()

    @abstractmethod
    def generate_upsert_stmt(self, table: Table, business_key_columns: List[str], columns: List[str]) -> Any:
        raise NotImplementedError

    @abstractmethod
    def execute(self, statement: Any, records: List[Dict[str, Any]]) -> CursorResult:
        self.connection.execute(statement, records)

    @staticmethod
    def get_driver_name(db_type: str) -> str:
        if db_type == DbType.MYSQL.value:
            return "mysql+pymysql"

        return db_type


class PostgresConnector(Connector):
    def generate_upsert_stmt(self, table: Table, business_key_columns: List[str], columns: List[str]) -> Any:
        from sqlalchemy.dialects.postgresql import insert

        insert_stmt = insert(table).values({col: "?" for col in columns})
        return insert_stmt.on_conflict_do_update(
            index_elements=[table.columns[column] for column in business_key_columns],
            set_={col: getattr(insert_stmt.excluded, col) for col in columns})

    def execute(self, statement: Any, records: List[Dict[str, Any]]) -> CursorResult:
        from psycopg2 import OperationalError

        try:
            return super().execute(statement, records)
        except OperationalError as e:
            raise ConnectionError(e)


class MySQLConnector(Connector):
    def generate_upsert_stmt(self, table: Table, business_key_columns: List[str], columns: List[str]) -> Any:
        from sqlalchemy.dialects.mysql import insert

        insert_stmt = insert(table).values({col: "?" for col in columns})
        return insert_stmt.on_duplicate_key_update(
            ColumnCollection(columns=[(x.name, x) for x in [insert_stmt.inserted[column] for column in columns]]))

    def execute(self, statement: Any, records: List[Dict[str, Any]]) -> CursorResult:
        from sqlalchemy.exc import OperationalError

        try:
            return super().execute(statement, records)
        except OperationalError as e:
            raise ConnectionError(e)
