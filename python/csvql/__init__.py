"""
csvql — fast CSV querying from Python.

Usage::

    import csvql

    # Returns a list of dicts
    rows = csvql.query("SELECT category, COUNT(*) FROM 'sales.csv' GROUP BY category")
    # [{"category": "Electronics", "COUNT(*)": "42"}, ...]

    # Returns raw CSV string (header + rows)
    csv_text = csvql.query_csv("SELECT * FROM 'data.csv' WHERE amount > 100")

    # Optional pandas integration (pandas not required)
    df = csvql.query_df("SELECT region, SUM(revenue) FROM 'data.csv' GROUP BY region")
"""

import ctypes
import csv
import io
import json
from typing import Any

from ._loader import load


def query(sql: str) -> list[dict[str, Any]]:
    """Run a SQL query and return results as a list of dicts.

    Args:
        sql: SQL string, e.g. ``"SELECT * FROM 'data.csv' WHERE age > 30"``

    Returns:
        List of row dicts, e.g. ``[{"name": "Alice", "age": "31"}, ...]``
        All values are strings (CSV has no type information).

    Raises:
        RuntimeError: if the query fails (bad SQL, file not found, etc.)
    """
    lib = load()
    out = ctypes.c_void_p(None)
    rc = lib.csvql_query_json(sql.encode(), ctypes.byref(out))
    addr = out.value
    if rc != 0:
        raw = ctypes.string_at(addr) if addr else b"unknown error"
        if addr:
            lib.csvql_free(addr)
        raise RuntimeError(raw.decode(errors="replace"))
    if not addr:
        return []
    raw = ctypes.string_at(addr)
    lib.csvql_free(addr)
    return json.loads(raw.decode())


def query_csv(sql: str) -> str:
    """Run a SQL query and return results as a CSV string (header + rows).

    Args:
        sql: SQL string.

    Returns:
        CSV text with a header row, e.g. ``"name,age\\nAlice,31\\n"``

    Raises:
        RuntimeError: if the query fails.
    """
    lib = load()
    out = ctypes.c_void_p(None)
    rc = lib.csvql_query_csv(sql.encode(), ctypes.byref(out))
    addr = out.value
    if rc != 0:
        raw = ctypes.string_at(addr) if addr else b"unknown error"
        if addr:
            lib.csvql_free(addr)
        raise RuntimeError(raw.decode(errors="replace"))
    if not addr:
        return ""
    raw = ctypes.string_at(addr)
    lib.csvql_free(addr)
    return raw.decode()


def query_df(sql: str):
    """Run a SQL query and return a pandas DataFrame.

    Requires pandas to be installed. All columns will be ``object`` dtype
    (strings) — cast as needed after calling this function.

    Args:
        sql: SQL string.

    Returns:
        ``pandas.DataFrame``

    Raises:
        ImportError: if pandas is not installed.
        RuntimeError: if the query fails.
    """
    try:
        import pandas as pd
    except ImportError as e:
        raise ImportError("pandas is required for query_df(). Install it with: pip install pandas") from e

    csv_text = query_csv(sql)
    return pd.read_csv(io.StringIO(csv_text))


def query_tuples(sql: str) -> tuple[list[str], list[tuple[str, ...]]]:
    """Run a SQL query and return (headers, rows) as plain tuples.

    Useful when you want array-of-arrays access (``row[0]``) instead of
    dicts, or when memory overhead of dicts matters.

    Args:
        sql: SQL string.

    Returns:
        A ``(headers, rows)`` tuple where ``headers`` is a list of column
        name strings and ``rows`` is a list of string tuples — one per row.

    Raises:
        RuntimeError: if the query fails.

    Example::

        headers, rows = csvql.query_tuples("SELECT name, age FROM 'data.csv'")
        # headers: ['name', 'age']
        # rows:    [('Alice', '30'), ('Bob', '25'), ...]
        for row in rows:
            print(row[0], row[1])
    """
    csv_text = query_csv(sql)
    if not csv_text:
        return [], []
    reader = csv.reader(io.StringIO(csv_text))
    all_rows = list(reader)
    if not all_rows:
        return [], []
    headers = all_rows[0]
    data = [tuple(r) for r in all_rows[1:] if r]
    return headers, data


__all__ = ["query", "query_csv", "query_df", "query_tuples"]
