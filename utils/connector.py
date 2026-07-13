import duckdb

_con = None

def get_connection(catalog_path: str) -> duckdb.DuckDBPyConnection:
    global _con
    if _con is None:
        _con = duckdb.connect(catalog_path)
        _con.execute("INSTALL ducklake; LOAD ducklake;")
    return _con

def close_connection():
    global _con
    if _con:
        _con.close()
        _con = None
