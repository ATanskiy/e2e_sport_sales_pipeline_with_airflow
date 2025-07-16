"""
Schema and Table Creation Utility for PostgreSQL

This script initializes the required database schemas and tables across multiple
environments (e.g., `raw`, `prod`, `playground`) by parsing SQL definitions from
a single SQL file.

Key Features:
- Reads a full SQL file containing multiple `CREATE TABLE` statements using the placeholder `{{schema}}`
- Extracts each `CREATE TABLE` block and maps it to a table name
- Iterates over all schemas defined in the config
    - Creates schemas if they do not exist
    - Checks if each table exists in each schema
    - Creates any missing tables using the parsed SQL, replacing `{{schema}}` with the actual name

Functions:
- `extract_all_table_sql()` — Parses the raw SQL file into `{table_name: sql_block}` mappings
- `create_schemas_tables()` — Orchestrates schema creation and table setup across all configured schemas
"""

import re
from db.connection import get_connection
from configs.config import SCHEMAS, CREATE_TABLES_SCHEMAS_PATH

def extract_all_table_sql(sql_text):
    """
    Extracts all CREATE TABLE blocks into a dictionary: { table_name: create_sql }
    Assumes table blocks start with CREATE TABLE IF NOT EXISTS {{schema}}.table_name
    and end with ');'
    """
    table_sql_map = {}
    current_block = []
    current_table = None
    inside_block = False

    for line in sql_text.splitlines():
        if line.strip().upper().startswith("CREATE TABLE {{SCHEMA}}."):
            inside_block = True
            current_block = [line]
            # extract table name
            match = re.search(r'CREATE TABLE \{\{schema\}\}\.(\w+)', line)
            if match:
                current_table = match.group(1)
        elif inside_block:
            current_block.append(line)
            if line.strip().endswith(");"):
                if current_table:
                    table_sql_map[current_table] = "\n".join(current_block)
                current_table = None
                current_block = []
                inside_block = False

    return table_sql_map


def create_schemas_tables():
    with open(CREATE_TABLES_SCHEMAS_PATH, "r") as f:
        full_sql = f.read()

    # Parse individual CREATE TABLE blocks
    table_sql_map = extract_all_table_sql(full_sql)

    conn = get_connection()
    cur = conn.cursor()

    tables = list(table_sql_map.keys())

    for schema in SCHEMAS:
        # Create schema if needed
        cur.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s", (schema,))
        if cur.fetchone():
            print(f"Schema '{schema}' exists.")
        else:
            cur.execute(f"CREATE SCHEMA {schema}")
            print(f"Schema '{schema}' created.")

        # Check and create tables
        for table in tables:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = %s AND table_name = %s
                )
            """, (schema, table))
            exists = cur.fetchone()[0]

            if exists:
                print(f"Table '{table}' exists in schema '{schema}'.")
            else:
                print(f"Table '{table}' created in schema '{schema}'.")
                sql = table_sql_map[table].replace("{{schema}}", schema)
                cur.execute(sql)

    conn.commit()
    cur.close()
    conn.close()
    print("Done.")