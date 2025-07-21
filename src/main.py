# main.py
import argparse

from dems.mcp import run_server


def main():
    parser = argparse.ArgumentParser(description="Start the MCP server with a DuckDB database.")
    parser.add_argument(
        "--db",
        help="Path to the DuckDB database",
        default="test_sample_db.duckdb",
    )
    args = parser.parse_args()
    run_server(args.db)


if __name__ == "__main__":
    main()
