"""Optional Parquet export and DuckDB query surface.

Requires the ``parquet`` extra::

    pip install get-weather-data[parquet]

The daily and hourly DataFrames already write Parquet via pandas
(``get_frame(...).to_parquet(path)``); the streaming CSV batch path
writes Parquet directly (no pandas) when the output ends in
``.parquet``. This module adds :func:`query_weather`, a thin DuckDB
helper for running SQL over one or more Parquet files.
"""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pandas as pd


def _require_duckdb() -> "Any":
    """Import duckdb or raise a helpful error.

    Returns:
        The imported duckdb module.

    Raises:
        ImportError: If duckdb is not installed.
    """
    try:
        import duckdb
    except ImportError as exc:  # pragma: no cover - exercised via message test
        raise ImportError(
            "duckdb is required for query_weather. Install it with "
            "'pip install get-weather-data[parquet]'."
        ) from exc
    return duckdb


def query_weather(
    sql: str,
    tables: dict[str, str] | None = None,
    as_frame: bool = False,
) -> "list[dict[str, Any]] | pd.DataFrame":
    """Run a SQL query over Parquet files with DuckDB.

    Each entry in ``tables`` is registered as a view of that name over
    the given Parquet path (a single file or a glob like
    ``"out/*.parquet"``), so the query can join and aggregate across
    many exported files — the analytical "millions of rows" workflow.

    Args:
        sql: A DuckDB SQL query referencing the registered view names.
        tables: View name -> Parquet path or glob. View names must be
            valid SQL identifiers. When None, the query must be
            self-contained (e.g. ``SELECT * FROM read_parquet('a.parquet')``).
        as_frame: If True, return a pandas DataFrame (requires pandas);
            otherwise return a list of row dicts.

    Returns:
        The query result as a list of row dicts, or a DataFrame when
        ``as_frame`` is set.

    Raises:
        ImportError: If duckdb (or, for as_frame, pandas) is missing.
        ValueError: If a view name is not a valid SQL identifier.
    """  # noqa: DOC501,DOC502,DOC503 - ImportError raised by _require_duckdb / duckdb.df
    duckdb = _require_duckdb()
    con = duckdb.connect(":memory:")
    try:
        for name, source in (tables or {}).items():
            if not name.isidentifier():
                raise ValueError(f"invalid view name: {name!r}")
            # DDL can't bind a prepared parameter, so inline the path as a
            # single-quote-escaped literal (view name validated above).
            literal = str(source).replace("'", "''")
            con.execute(
                f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{literal}')"  # noqa: S608
            )
        result = con.execute(sql)
        if as_frame:
            return result.df()
        columns = [d[0] for d in result.description]
        return [dict(zip(columns, row, strict=True)) for row in result.fetchall()]
    finally:
        con.close()
