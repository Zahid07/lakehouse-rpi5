import re
from pathlib import Path
from utils.connector import get_connection


def load_sql(filepath: str, params: dict) -> str:
    """Load SQL file and replace :param placeholders."""
    sql = Path(filepath).read_text()
    for key, value in params.items():
        sql = sql.replace(f":{key}", str(value))
    # Warn about any unreplaced params
    remaining = re.findall(r":[a-zA-Z_]+", sql)
    if remaining:
        print(f"  [WARN] Unreplaced params in {filepath}: {remaining}")
    return sql


def run_sql_file(filepath: str, params: dict, catalog_path: str):
    """Load, parameterize, and execute a SQL file."""
    print(f"  Running: {filepath}")
    sql = load_sql(filepath, params)
    con = get_connection(catalog_path)
    # Split on semicolons to run multiple statements
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    for stmt in statements:
        con.execute(stmt)
    print(f"  Done:    {filepath}")


def run_sql_string(sql: str, params: dict, catalog_path: str):
    """Run a raw SQL string with param replacement."""
    for key, value in params.items():
        sql = sql.replace(f":{key}", str(value))
    con = get_connection(catalog_path)
    return con.execute(sql)
