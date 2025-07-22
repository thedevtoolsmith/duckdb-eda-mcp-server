# DuckDB EDA MCP Server (DEMS)

## About

DEMS is intended to be helpful for anyone who wants to do exploratory data analytics, i.e., do some quick and dirty
analytics of some data to present an argument or to prove a point using DuckDB. (If you're not using DuckDB, [check it
out](https://duckdb.org/). It's a *really* fast columnar, in-process DB)

You might be a developer/product manager who wants to make a business case for building a new feature, or somebody
importing and analysing some data for writing a blog post. This MCP server is designed to help you quickly sift through
the data.

## Features

- Tool to help import data from CSV and JSON files. *Don't spend valuable credits asking LLMs to generate queries to
  import data.*
- DELETE, DROP and UPDATE queries are automatically blocked; *LLMs cannot mess up your data “accidentally.”*
- Tool to validate SQL queries; *This helps LLMs know when it generates garbage.*
- Tools that are tailored for exploratory data analytics using DuckDB like `get_sample_data`,
  `get_table_schema_and_stats`. *Helps the LLM leverage DuckDB capabilities instead of generating the same SQL over and
  over again.*
- Default timeout of 60 seconds for all queried. *This will save you if LLMs manage to mess up in spite of all the other
  safeguards.*

## Server Installation

This MCP has to be installed and run locally, as DuckDB will be present in your file system (Data privacy is an
unintended side effect). The recommended way to run this locally is using `uv`

```shell
uvx --from git+https://github.com/thedevtoolsmith/duckdb-eda-mcp-server dems --db <DUCKDB_FILE_PATH>
```

## Integration with Client

The MCP server can be integrated into an MCP client by copying and pasting the following config block into your config
file:

```json
{
  "mcpServers": {
    "dems": {
      "command": "npx",
      "args": [
        "mcp-remote",
        "http://localhost:8000/sse"
      ]
    }
  }
}
```

## Roadmap

If there is enough interest, the following features are planned:

- [ ] Add the DuckDB documentation as a resource
- [ ] Add tools to give a column summary
- [ ] Add prompts to help users with EDA-related use cases