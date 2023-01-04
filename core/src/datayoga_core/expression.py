import json
import logging
import sqlite3
from abc import abstractmethod
from collections.abc import MutableMapping
from enum import Enum, unique
from typing import Any, Dict, List, Union

import jmespath
from datayoga_core.jmespath_custom_functions import JmespathCustomFunctions

logger = logging.getLogger("dy")


@unique
class Language(Enum):
    JMESPATH = "jmespath"
    SQL = "sql"


def flatten_data(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # flattened structure
    data_inner = data if isinstance(data, list) else [data]
    data_inner = [flatten(row, sep=".") for row in data_inner]
    return data_inner


def flatten(d, parent_key="", sep="_"):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


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
            return [row[0] if row[1] else None for row in zip(data,self.search_bulk(data))]
        else:
            return [row[0] for row in zip(data,self.search_bulk(data)) if row[1]]

class SQLExpression(Expression):
    def compile(self, expression: str):
        # we turn off `check_same_thread` to gain performance benefit by reusing the same connection object
        # safe to use since we are only creating in memory structures
        self.conn = sqlite3.connect(":memory:", check_same_thread=False)
        # we support both single field expressions and multiple fields
        self._is_single_field = True
        try:
            self._fields = json.loads(expression)
            self._is_single_field = False
        except json.JSONDecodeError:
            # this is not a json, treat as a simple expression
            self._fields = {"expr": expression}


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
        # create the in memory data structure
        data_inner = flatten_data(data)
        # builds an expression for fetching in memory data
        column_names = data_inner[0].keys()
        columns_clause = ','.join(f"[column{i+1}] as `{col}`" for i, col in enumerate(column_names))

        # values in the form of (?,?), (?,?)
        values_clause_row = f"({','.join('?' * len(column_names))})"
        values_clause = ','.join([values_clause_row] * len(data_inner))

        subselect = f"select {columns_clause} from (values {values_clause})"

        # bind the variables
        data_values = [row.get(colname) for row in data_inner for colname in column_names]

        # expressions clause
        expressions_clause = ", ".join([f"{expression} as `{column_name}`" for column_name, expression in expressions.items()])
        self.conn.row_factory = sqlite3.Row
        # we don't use CTE because of compatibility with older SQLlite versions on Centos7
        statement = f"select {expressions_clause} from ({subselect})"

        logger.debug(statement)
        cursor = self.conn.execute(statement, data_values)
        return [dict(x) for x in cursor.fetchall()]


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
    if language == Language.JMESPATH.value:
        expression_class = JMESPathExpression()
    elif language == Language.SQL.value:
        expression_class = SQLExpression()
    else:
        raise ValueError(f"unknown expression language {language}")

    expression_class.compile(expression)
    return expression_class
