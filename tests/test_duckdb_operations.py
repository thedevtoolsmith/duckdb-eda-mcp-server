import os

import duckdb
import pytest

from src.dems.database import DuckDB
from src.dems.sample_db import (
    create_db_with_sample_data,
    create_sample_csv,
    create_sample_json,
)


@pytest.fixture(scope="session")
def valid_db_path():
    """
    Create an empty valid DuckDB database at the beginning of the test session.
    The database will be automatically cleaned up after all tests are run.
    """
    db_path = "test_valid_db.duckdb"

    # Remove the database file if it already exists
    if os.path.exists(db_path):
        os.remove(db_path)

    conn = duckdb.connect(db_path, read_only=False)
    conn.close()

    yield db_path

    # Cleanup after all tests
    if os.path.exists(db_path):
        os.remove(db_path)


@pytest.fixture(scope="session")
def sample_db_path():
    """
    Create a sample DuckDB database with test data at the beginning of the test session.
    The database will be automatically cleaned up after all tests are run.
    """
    db_path = "test_sample_db.duckdb"
    create_db_with_sample_data(db_path)
    yield db_path

    # Cleanup after all tests
    if os.path.exists(db_path):
        os.remove(db_path)


def test_open_database_valid_path(valid_db_path):
    # Create a database directly using DuckDB
    duckdb_instance = DuckDB(valid_db_path)
    assert duckdb_instance is not None


def test_open_database_invalid_path():
    non_existent_db_path = "non_existing_db.duckdb"
    with pytest.raises(FileNotFoundError):
        DuckDB(non_existent_db_path)


def test_open_database_invalid_duckdb_file():
    invalid_db_path = "invalid_db_path.duckdb"
    with open(invalid_db_path, "w"):
        ...
    with pytest.raises(IOError):
        DuckDB(invalid_db_path)
    os.remove(invalid_db_path)


def test_list_tables_empty_database(valid_db_path):
    duckdb_instance = DuckDB(valid_db_path)
    tables = duckdb_instance.list_tables()
    assert tables == []


def test_list_tables_populated_database(sample_db_path):
    duckdb_instance = DuckDB(sample_db_path)
    tables = duckdb_instance.list_tables()
    assert len(tables) == 3
    assert set(tables) == {"authors", "books", "reviews"}


def test_execute_query(sample_db_path):
    duckdb_instance = DuckDB(sample_db_path)
    # Test a simple query
    result = duckdb_instance.safe_execute_query("SELECT COUNT(*) FROM authors")
    assert result[0][0] == 5

    # Test query with parameters
    result = duckdb_instance.safe_execute_query("SELECT * FROM authors WHERE country = 'United Kingdom'")
    assert len(result) == 2

    # Test query with forbidden operations
    with pytest.raises(ValueError):
        duckdb_instance.safe_execute_query("DELETE FROM authors WHERE author_id = 1")

    with pytest.raises(ValueError):
        duckdb_instance.safe_execute_query("DROP TABLE authors")

    with pytest.raises(ValueError):
        duckdb_instance.safe_execute_query("UPDATE authors SET country = 'USA' WHERE author_id = 1")


def test_get_sample_data(sample_db_path):
    duckdb_instance = DuckDB(sample_db_path)
    # Test the default number of rows
    result = duckdb_instance.get_sample_data("books")
    assert len(result) == 8  # All books since there are only 8

    # Test with a specific number of rows
    result = duckdb_instance.get_sample_data("books", num_rows=3)
    assert len(result) == 3

    # Test with table that doesn't exist
    with pytest.raises(Exception):
        duckdb_instance.get_sample_data("non_existent_table")


def test_get_table_schema_and_stats(sample_db_path):
    duckdb_instance = DuckDB(sample_db_path)
    schema_stats = duckdb_instance.get_table_schema_and_stats("books")

    # Check schema information
    assert schema_stats["table_name"] == "books"
    assert schema_stats["row_count"] == 8

    # Check columns
    columns = schema_stats["columns"]
    assert len(columns) == 6

    # Verify column names
    column_names = [col["name"] for col in columns]
    assert set(column_names) == {"book_id", "title", "author_id", "publication_year", "genre", "price"}

    # Check statistics
    stats = schema_stats["statistics"]
    assert len(stats) > 0

    # Test with a table that doesn't exist
    with pytest.raises(Exception):
        duckdb_instance.get_table_schema_and_stats("non_existent_table")


def test_import_data():
    csv_path = "test_data.csv"
    json_path = "test_data.json"
    db_path = "test_import.duckdb"
    create_sample_csv(csv_path)
    create_sample_json(json_path)
    if os.path.exists(db_path):
        os.remove(db_path)

    try:
        # Create an empty database file
        conn = duckdb.connect(db_path, read_only=False)
        conn.close()

        duckdb_instance = DuckDB(db_path)
        # Test CSV import
        result = duckdb_instance.import_data("csv", "csv_table", csv_path)
        assert result is True

        # TODO: Verify all the data as part of the tests
        data = duckdb_instance.get_sample_data("csv_table")
        assert len(data) == 2

        # Test JSON import
        result = duckdb_instance.import_data("json", "json_table", json_path)
        assert result is True

        # TODO: Verify all the data as part of the tests
        data = duckdb_instance.get_sample_data("json_table")
        assert len(data) == 2

        # Test with invalid format
        with pytest.raises(ValueError):
            duckdb_instance.import_data("invalid_format", "invalid_table", csv_path)

        # Test with a non-existent file
        with pytest.raises(FileNotFoundError):
            duckdb_instance.import_data("csv", "missing_file_table", "non_existent_file.csv")
    finally:
        # Clean up
        os.remove(csv_path)
        os.remove(json_path)
        if os.path.exists(db_path):
            os.remove(db_path)


def test_is_valid_sql(sample_db_path):
    duckdb_instance = DuckDB(sample_db_path)

    # Test valid SQL
    assert duckdb_instance.is_valid_sql("SELECT * FROM books") is True

    # Test invalid SQL
    assert duckdb_instance.is_valid_sql("SELECT * FROM non_existent_table") is False
    assert duckdb_instance.is_valid_sql("NOT A VALID SQL QUERY") is False
