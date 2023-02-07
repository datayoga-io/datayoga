import json
import logging

import sqlglot

try:
    # older linux doesn't have adequate version
    import pysqlite3 as sqlite3
except ImportError:
    import sqlite3

from abc import abstractmethod
from enum import Enum, unique
from typing import Any, Dict, List, Tuple, Union

import jmespath
from datayoga_core.jmespath_custom_functions import JmespathCustomFunctions

logger = logging.getLogger("dy")


@unique
class Language(str, Enum):
    JMESPATH = "jmespath"
    SQL = "sql"


def get_nested_value(data: Dict[str, Any], key: Tuple[str]) -> Any:
    # get nested key
    for level in key:
        try:
            data = data[level]
        except KeyError:
            # such field does not exist, return None
            return None
    return data


class Expression():
    @abstractmethod
    def compile(self, expression: str):
        """Compiles an expression

        Args:
            expression (str): expression
        """
        raise NotImplementedError

    @abstractmethod
    def search(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Executes the expression on a single data entry

        Args:
            data (List[Dict[str, Any]]): Data

        Returns:
            List[Dict[str, Any]]: Transformed data
        """
        raise NotImplementedError

    @abstractmethod
    def search_bulk(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Executes the expression in bulk on a list of data entries

        Args:
            data (List[Dict[str, Any]]): Data

        Returns:
            List[Dict[str, Any]]: Transformed data
        """
        raise NotImplementedError

    def filter(self, data: List[Dict[str, Any]], tombstone: bool = False) -> List[Union[Dict[str, Any], None]]:
        """Tests a where clause for an SQL statement

        Args:
            data (List[Dict[str, Any]]): Data
            tombestone: if True, returns None for filtered values

        Returns:
            List[Dict[str, Any]]: Filtered data
        """
        # filter is essentially using a single field returning 0 or 1 for the condition
        if tombstone:
            return [row[0] if row[1] else None for row in zip(data, self.search_bulk(data))]
        else:
            return [row[0] for row in zip(data, self.search_bulk(data)) if row[1]]


class SQLExpression(Expression):
    def compile(self, expression: str):
        # check min sqlite3 version to at least support values clause
        if sqlite3.sqlite_version_info < (3, 8, 8):
            raise ValueError(
                f"must have SQLite v3.8.8 and above to use SQL expressions. Found {sqlite3.sqlite_version_info}")

        # we turn off `check_same_thread` to gain performance benefit by reusing the same connection object
        # safe to use since we are only creating in memory structures
        self.conn = sqlite3.connect(":memory:", check_same_thread=False)
        # we support both single field expressions and multiple fields
        self._is_single_field = True
        try:
            self._fields = json.loads(expression)

            # verify this is a dict, and not a list or document ('x' is a valid json)
            if isinstance(self._fields, dict):
                self._is_single_field = False
            else:
                # this is not a dict, treat as a simple expression
                self._fields = {"expr": expression}

        except json.JSONDecodeError:
            # this is not a json, treat as a simple expression
            self._fields = {"expr": expression}

        # for each of the expressions, we determine the column names used.
        # they will get parsed and binded for performance instead of traversing the entire payload
        self._column_names = set()
        for _exp in self._fields.values():
            try:
                self._column_names.update(
                    [tuple(column.sql().replace('"', "").split("."))
                     for column in sqlglot.parse_one("SELECT " + _exp.replace('`', '"')).find_all(sqlglot.exp.Column)])
            except Exception:
                # a parse error
                raise ValueError(f"Cannot parse SQL expression: {_exp}")

    def search_bulk(self, data: List[Dict[str, Any]]) -> Any:
        results = self.exec_sql(data, self._fields)
        if self._is_single_field:
            # treat as a simple expression
            return [x.get("expr") for x in results]
        else:
            return results

    def search(self, data: Dict[str, Any]) -> Any:
        return self.search_bulk([data])[0]

    def exec_sql(self, data: List[Dict[str, Any]], expressions: Dict[str, str]) -> List[Dict[str, Any]]:
        """Executes an SQL statement

        Args:
            data (List[Dict[str, Any]]): Data
            expressions: Dict[str, str]: Expressions

        Returns:
            List[Dict[str, Any]]: Query result
        """
        # builds an expression for fetching in memory data
        self.conn.row_factory = sqlite3.Row

        if len(self._column_names) > 0:
            columns_clause = ','.join(f"[column{i+1}] as `{'.'.join(col)}`" for i, col in enumerate(self._column_names))

            # values in the form of (?,?), (?,?)
            values_clause_row = f"({','.join('?' * len(self._column_names))})"
            values_clause = ','.join([values_clause_row] * len(data))

            subselect = f"select {columns_clause} from (values {values_clause})"

            # bind the variables
            data_values = [get_nested_value(row, col) for row in data for col in self._column_names]

            # expressions clause
            expressions_clause = ", ".join(
                [f"{expression} as `{column_name}`" for column_name, expression in expressions.items()])
            # we don't use CTE because of compatibility with older SQLlite versions on Centos7
            statement = f"select {expressions_clause} from ({subselect})"

            logger.debug(statement)
            cursor = self.conn.execute(statement, data_values)
            return [dict(x) for x in cursor.fetchall()]
        else:
            # a sepcial case where we are only selecting literals. e.g. select current_timstamp or 'x'
            # no need to bind anything. just run it once and copy to all records
            expressions_clause = ", ".join(
                [f"{expression} as `{column_name}`" for column_name, expression in expressions.items()])
            cursor = self.conn.execute(f"select {expressions_clause}")
            return [dict(cursor.fetchone())] * len(data)


class JMESPathExpression(Expression):
    # register custom functions
    options = jmespath.Options(custom_functions=JmespathCustomFunctions())

    def compile(self, expression: str):
        self.expression = jmespath.compile(expression)

    def search(self, data: Dict[str, Any]) -> Any:
        return self.expression.search(data, options=self.options)

    def search_bulk(self, data: List[Dict[str, Any]]) -> Any:
        return [self.search(row) for row in data]


def compile(language: Language, expression: str) -> Expression:
    """Gets a compiled expression class based on the language

    Args:
        language (Language): Language
        expression (str): Expression

    Returns:
        Expression: Expression class
    """
    if language == Language.JMESPATH:
        expression_class = JMESPathExpression()
    elif language == Language.SQL:
        expression_class = SQLExpression()
    else:
        raise ValueError(f"unknown expression language {language}")

    expression_class.compile(expression)
    return expression_class
