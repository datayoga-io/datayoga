import json
import logging
import sqlite3
from enum import Enum, unique
from typing import Any, Dict, List

import jmespath
from datayoga_core.jmespath_custom_functions import JmespathCustomFunctions

logger = logging.getLogger("dy")


@unique
class Language(Enum):
    JMESPATH = "jmespath"
    SQL = "sql"


class Expression():
    def compile(self, expression: str):
        """Compiles an expression

        Args:
            expression (str): expression
        """
        pass

    def search(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Executes the expression on a given data

        Args:
            data (List[Dict[str, Any]]): Data

        Returns:
            List[Dict[str, Any]]: Transformed data
        """
        pass


class SQLExpression(Expression):
    def compile(self, expression: str):
        # we turn off check_same_thread to gain performance benefit by reusing the same connection object. safe to use since we are only creating in memory structures
        self.conn = sqlite3.connect(":memory:", check_same_thread=False)
        self.expression = expression

    def filter(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Tests a where clause for an SQL statement

        Args:
            data (List[Dict[str, Any]]): Data

        Returns:
            List[Dict[str, Any]]: Filtered data
        """
        # use a CTE to create the in memory data structure
        cte_clause = self._get_cte(data)

        column_names = data[0].keys()
        # fetch the CTE and bind the variables
        data_values = [row.get(colname) for row in data for colname in column_names]
        self.conn.row_factory = sqlite3.Row

        cursor = self.conn.execute(
            f"{cte_clause} select * from data where {self.expression}", data_values
        )
        return [dict(row) for row in cursor.fetchall()]

    def _get_cte(self, data: List[Any]) -> str:
        # builds a CTE expression for fetching in memory data

        column_names = data[0].keys()
        columns_clause = ','.join(f"`{col}`" for col in column_names)

        # values in the form of (?,?), (?,?)
        values_clause_row = f"({','.join('?'*len(column_names))})"
        values_clause = ','.join([values_clause_row]*len(data))

        # use a CTE to create the in memory data structure
        return f"with data({columns_clause}) as (values {values_clause})"

    def search(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        try:
            fields = json.loads(self.expression)
            new_data = {}
            for field in fields:
                new_data[field] = self.exec_sql(data, fields[field])
            return new_data
        except json.JSONDecodeError:
            # this is not a json, treat as a simple expression
            return self.exec_sql(data, self.expression)

    def exec_sql(self, data: List[Dict[str, Any]], expression: str) -> List[Dict[str, Any]]:
        """Executes an SQL statement

        Args:
            data (List[Dict[str, Any]]): Data

        Returns:
            List[Dict[str, Any]]: Query result
        """
        # use a CTE to create the in memory data structure
        data_inner = data if isinstance(data, list) else [data]
        cte_clause = self._get_cte(data_inner)

        column_names = data_inner[0].keys()

        # fetch the CTE and bind the variables
        data_values = [row.get(colname) for row in data_inner for colname in column_names]
        self.conn.row_factory = sqlite3.Row
        logger.debug(f"{cte_clause} select {expression} from data)")
        cursor = self.conn.execute(
            f"{cte_clause} select {expression} from data", data_values
        )

        return cursor.fetchone()[0]


class JMESPathExpression(Expression):
    # register custom functions
    options = jmespath.Options(custom_functions=JmespathCustomFunctions())

    def compile(self, expression: str):
        self.expression = jmespath.compile(expression)
        self.filter_expression = jmespath.compile(f"[?{expression}]")

    def filter(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return self.filter_expression.search(data, options=self.options)

    def search(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return self.expression.search(data, options=self.options)


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

    expression_class.compile(expression)
    return expression_class
