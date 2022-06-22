import json
import sqlite3
from enum import Enum, unique
from typing import Any

import jmespath


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

    def search(self, data: Any) -> Any:
        """Executes the expression on a given data

        Args:
            data (Any): data

        Returns:
            Any: transformed data
        """
        pass


class SQLExpression(Expression):
    def compile(self, expression: str):
        self.conn = sqlite3.connect(":memory")
        self.expression = expression

    def search(self, data: Any) -> Any:
        try:
            fields = json.loads(self.expression)
            new_data = {}
            for field in fields:
                new_data[field] = self.exec_sql(data, fields[field])
            return new_data
        except:
            return self.exec_sql(data, self.expression)

    def exec_sql(self, data: Any, expression: str) -> Any:
        """Executes an SQL statement

        Args:
            data (Any): Data

        Returns:
            Any: Query result
        """
        clauses = []
        for k, v in data.items():
            clauses.append(f"'{v}' as '{k}'")

        from_clause = f"select {','.join(clauses)}"

        return self.conn.execute(f"select {expression} from ({from_clause})").fetchone()[0]


class JMESPathExpression(Expression):
    def compile(self, expression: str):
        self.expression = jmespath.compile(expression)

    def search(self, data: Any) -> Any:
        return self.expression.search(data)


def get_expression_class(language: Language, expression: str) -> Expression:
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
