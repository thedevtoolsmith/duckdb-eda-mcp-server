import logging
import os
from pathlib import Path
from sys import stdout
from typing import Annotated, Literal

from fastmcp import Context, FastMCP
from pydantic import Field

from .database import DuckDB


def run_server(database_path: str):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(stdout)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    logger.info(f"Starting MCP server with database: {database_path}")
    server = FastMCP("DEMS")
    db = DuckDB(database_path)

    @server.tool(
        description="Execute the given SQL query and return the result.",
        annotations={"title": "Execute SQL query", "readOnly": True, "idempotentHint": True},
    )
    def execute_query(query: Annotated[str, Field(description="The SQL query to be executed.")]):
        db.safe_execute_query(query)

    @server.tool(
        description="Get all the tables with their schema and some statistical information about the data in them.",
        annotations={"title": "Generate DB Summary", "readOnly": True, "idempotentHint": True},
    )
    async def generate_db_summary(ctx: Context) -> str:
        stats = {table: db.get_table_schema_and_stats(table) for table in db.list_tables()}
        await ctx.info("Got stats. Now, converting it to natural language.")
        prompt = (
            f"The data that I am about to pass contains a list of all the tables in a database and its columns, "
            f"their data types and some stats regarding the data. Convert the given data into natural language, "
            f"so it is easier for a person to understand the table and the database structure. "
            f"Here is the data {stats}."
        )
        response = await ctx.sample(prompt)
        return response.text

    @server.tool(
        description="Get a list of all the user defined tables in the database.",
        annotations={"title": "Get all the Tables", "readOnlyHint": True, "idempotentHint": True},
    )
    def get_tables() -> list:
        return db.list_tables()

    @server.tool(
        description="Validate if an SQL query is error-free and will run without errors.",
        annotations={"title": "Validate SQL Query", "readOnlyHint": True, "idempotentHint": True},
    )
    def validate_query(
            query: Annotated[str, Field(description="The SQL query for which validity check has to be performed.")],
    ) -> bool:
        return db.is_valid_sql(query)

    @server.tool(
        description="Import data from a CSV or JSON file in the filesystem into a table.",
        annotations={"title": "Import Data from Filesystem", "readOnlyHint": True, "idempotentHint": True},
    )
    def import_data(
            file_type: Annotated[
                Literal["csv", "json"], Field(
                    description="The file type to be imported. Only CSV and JSON is supported.")
            ],
            table_name: Annotated[str, Field(description="The name of the table that will be created.")],
            file_path: Annotated[Path, Field(description="The file path that will have the data.")],
    ) -> bool:
        if file_type.strip().lower() not in {"csv", "json"}:
            raise ValueError("Unsupported file format. Supported formats are CSV and JSON.")

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        return db.import_data(file_type, table_name, file_path)

    server.run()
