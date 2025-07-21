import os
import re
from concurrent import futures

import duckdb

from .sample_db import create_db_with_sample_data


class DuckDBException(Exception):
    pass


class DuckDB:
    def __init__(self, database_path: str, timeout_in_seconds: float = 60.0) -> None:
        self.database_path = database_path
        self.connection = self.open_database(self.database_path)
        self.timeout_in_seconds = timeout_in_seconds

    @staticmethod
    def open_database(database_path: str) -> duckdb.DuckDBPyConnection:
        """
        Open a connection to a DuckDB database.

        :param database_path: Path to the DuckDB database file.
        :type database_path: str
        :return: A connection object to the DuckDB database.
        :rtype: duckdb.DuckDBPyConnection
        :raises FileNotFoundError: If the specified database file does not
            exist.
        """
        if not os.path.exists(database_path):
            if database_path == "test_sample_db.duckdb":
                create_db_with_sample_data(database_path)
            else:
                raise FileNotFoundError(f"Database file not found: {database_path}")

        try:
            connection = duckdb.connect(database_path)
        except duckdb.IOException as err:
            raise IOError(f"Error opening database: {err}")

        return connection

    def safe_execute_query(self, query: str) -> list | None:
        """
        Execute an SQL query in DuckDB.

        Args:
            query (str): SQL query to execute

        Returns:
            The result of the query execution

        Raises:
            ValueError: If DELETE or DROP statements are detected
        """

        if re.search(r"\b(DELETE|DROP|UPDATE)\b", query, re.IGNORECASE):
            raise ValueError("DELETE, DROP and UPDATE operations are not allowed")

        # Exceptions will be propagated to the callers for them to handle.
        def _run():
            if not query.strip().upper().startswith("INSERT"):
                return self.connection.execute(query).fetchall()
            else:
                return self.connection.execute(query)

        with futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_run)
            try:
                return future.result(timeout=self.timeout_in_seconds)
            except futures.TimeoutError as exc:
                # Interrupt the running query and propagate a TimeoutError
                self.connection.interrupt()
                raise TimeoutError(
                    f"Query exceeded timeout of {self.timeout_in_seconds} s and was interrupted."
                ) from exc

    def list_tables(self) -> list[str]:
        """
        List all tables in the DuckDB database.

        Returns:
            list: List of table names
        """
        query = "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
        results = self.safe_execute_query(query)
        return [table[0] for table in results]

    def get_sample_data(self, table_name, num_rows=10):
        """
        Get a sample of data from the specified table.

        Args:
            table_name (str): Name of the table
            num_rows (int, optional): Number of rows to return (default: 10)

        Returns:
            list: Sample data from the table
        """
        try:
            query = f"SELECT * FROM {table_name} LIMIT {num_rows}"
            return self.safe_execute_query(query)
        except duckdb.CatalogException as err:
            raise DuckDBException(err)

    def get_table_schema_and_stats(self, table_name):
        """
        Get the schema and statistics for a table.

        Args:
            table_name (str): Name of the table

        Returns:
            dict: Table schema and statistics
        """
        # Get schema
        schema_query = f"PRAGMA table_info({table_name})"
        schema_result = self.safe_execute_query(schema_query)

        columns = []
        for col_data in schema_result:
            columns.append(
                {
                    "name": col_data[1],
                    "type": col_data[2],
                    "not_null": bool(col_data[3]),
                    "primary_key": bool(col_data[5]),
                }
            )

        count_query = f"SELECT COUNT(*) FROM {table_name}"
        row_count = self.safe_execute_query(count_query)[0][0]

        stats_query = f"SUMMARIZE {table_name}"
        stats_result = self.safe_execute_query(stats_query)

        # TODO: Format stats based on the datatype. ex: return numeric related stats only if the column is of numeric types.
        stats = []
        for row in stats_result:
            stats.append(
                {
                    "column": row[0],
                    "count": row[1],
                    "count_distinct": row[2],
                    "null_count": row[3],
                    "min": row[4],
                    "max": row[5],
                    "avg": row[6],
                    "std": row[7],
                    "q25": row[8],
                    "q50": row[9],  # median
                    "q75": row[10],
                    "mode": row[11] if len(row) > 11 else None,
                }
            )

        return {
            "table_name": table_name,
            "columns": columns,
            "row_count": row_count,
            "statistics": stats,
        }

    def import_data(self, format_type, table_name, file_path):
        """
        Import data from a CSV or JSON file into a table.

        Args:
            format_type (str): 'csv' or 'json'
            table_name (str): Name of the table to create or insert into
            file_path (Path): Path to the file to import

        Returns:
            bool: True if successful
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        if format_type.lower() == "csv":
            query = f"CREATE TABLE {table_name} AS SELECT * FROM read_csv('{file_path}'"
        elif format_type.lower() == "json":
            query = f"CREATE TABLE {table_name} AS SELECT * FROM read_json('{file_path}'"
        else:
            raise ValueError("Unsupported file format. Supported formats are CSV and JSON.")

        query += ")"

        self.safe_execute_query(query)
        return True

    def is_valid_sql(self, query):
        """
        Check if a query is valid SQL by attempting to explain it.

        Args:
            query (str): SQL query to validate

        Returns:
            bool: True if the query is valid, False otherwise
        """
        try:
            explainer = f"EXPLAIN (FORMAT JSON) {query}"
            self.connection.execute(explainer).fetchone()[0]
            return True
        # TODO: Explore returning the reason why the query is invalid
        except Exception:
            return False

    def close(self):
        self.connection.close()
