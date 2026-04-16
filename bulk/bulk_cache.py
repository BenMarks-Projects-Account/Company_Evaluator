"""
SQLite persistence for bulk data.

Stores filtered bulk DataFrames as SQLite tables on the NAS.
Provides lookup methods for downstream consumers and tracks
freshness with a _bulk_refresh_metadata table.
"""
import logging
import sqlite3
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

import pandas as pd

logger = logging.getLogger(__name__)


class BulkCache:
    """SQLite-backed cache for bulk FMP data."""

    METADATA_TABLE = "_bulk_refresh_metadata"

    def __init__(self, db_path: str):
        """Initialize with path to SQLite database."""
        self.db_path = db_path
        self._ensure_metadata_table()

    def _get_conn(self) -> sqlite3.Connection:
        """Create a new connection (SQLite is not thread-safe to share)."""
        return sqlite3.connect(self.db_path, timeout=30)

    def _ensure_metadata_table(self):
        """Create the metadata tracking table if it doesn't exist."""
        with self._get_conn() as conn:
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.METADATA_TABLE} (
                    endpoint_name TEXT,
                    part INTEGER,
                    year INTEGER,
                    table_name TEXT NOT NULL,
                    row_count INTEGER NOT NULL,
                    refreshed_at TIMESTAMP NOT NULL,
                    size_bytes INTEGER,
                    PRIMARY KEY (endpoint_name, part, year)
                )
            """)
            conn.commit()

    def store(
        self,
        endpoint_name: str,
        dataframe: pd.DataFrame,
        part: Optional[int] = None,
        year: Optional[int] = None,
    ):
        """Store a DataFrame into the cache."""
        table_name = self._table_name(endpoint_name, part, year)

        with self._get_conn() as conn:
            dataframe.to_sql(table_name, conn, if_exists="replace", index=False)

            conn.execute(
                f"""
                INSERT OR REPLACE INTO {self.METADATA_TABLE}
                (endpoint_name, part, year, table_name, row_count,
                 refreshed_at, size_bytes)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    endpoint_name,
                    part if part is not None else -1,
                    year if year is not None else -1,
                    table_name,
                    len(dataframe),
                    datetime.now().isoformat(),
                    int(dataframe.memory_usage(deep=True).sum()),
                ),
            )
            conn.commit()

        logger.info(
            f"Stored {endpoint_name}"
            + (f" part={part}" if part is not None else "")
            + (f" year={year}" if year is not None else "")
            + f": {len(dataframe)} rows -> {table_name}"
        )

    def _table_name(
        self,
        endpoint_name: str,
        part: Optional[int] = None,
        year: Optional[int] = None,
    ) -> str:
        """Generate a SQLite table name from endpoint + parameters."""
        parts = ["bulk", endpoint_name]
        if part is not None:
            parts.append(f"p{part}")
        if year is not None:
            parts.append(f"y{year}")
        return "_".join(parts)

    def get_dataframe(
        self,
        endpoint_name: str,
        part: Optional[int] = None,
        year: Optional[int] = None,
    ) -> Optional[pd.DataFrame]:
        """Retrieve a stored DataFrame from the cache."""
        table_name = self._table_name(endpoint_name, part, year)
        try:
            with self._get_conn() as conn:
                return pd.read_sql(f'SELECT * FROM "{table_name}"', conn)
        except Exception:
            return None

    def get_row_for_symbol(
        self,
        endpoint_name: str,
        symbol: str,
        part: Optional[int] = None,
        year: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        """Look up a single symbol's row in a cached table."""
        table_name = self._table_name(endpoint_name, part, year)
        try:
            with self._get_conn() as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    f'SELECT * FROM "{table_name}" WHERE UPPER(symbol) = ?',
                    (symbol.upper(),),
                )
                row = cursor.fetchone()
                return dict(row) if row else None
        except Exception:
            return None

    def get_all_rows_for_symbol(self, symbol: str) -> Dict[str, Any]:
        """
        Look up all cached data for a symbol across all endpoint tables.
        Returns a dict keyed by a descriptive label per table.
        """
        result: Dict[str, Any] = {}
        try:
            with self._get_conn() as conn:
                conn.row_factory = sqlite3.Row
                metadata_rows = conn.execute(
                    f"SELECT * FROM {self.METADATA_TABLE}"
                ).fetchall()

                for meta in metadata_rows:
                    meta = dict(meta)
                    table = meta["table_name"]
                    try:
                        row = conn.execute(
                            f'SELECT * FROM "{table}" WHERE UPPER(symbol) = ?',
                            (symbol.upper(),),
                        ).fetchone()
                        if row:
                            key = meta["endpoint_name"]
                            if meta["year"] != -1:
                                key += f"_{meta['year']}"
                            if meta["part"] != -1:
                                key += f"_part{meta['part']}"
                            result[key] = dict(row)
                    except Exception:
                        pass
        except Exception as e:
            logger.debug(f"get_all_rows_for_symbol error: {e}")
        return result

    def is_stale(self, hours: int = 24) -> bool:
        """Check if the cache is older than the given number of hours."""
        try:
            with self._get_conn() as conn:
                cursor = conn.execute(
                    f"SELECT MIN(refreshed_at) as oldest FROM {self.METADATA_TABLE}"
                )
                row = cursor.fetchone()
                if not row or not row[0]:
                    return True
                oldest = datetime.fromisoformat(row[0])
                return (datetime.now() - oldest) > timedelta(hours=hours)
        except Exception:
            return True

    def get_refresh_summary(self) -> Dict[str, Any]:
        """Return summary of cache state."""
        try:
            with self._get_conn() as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(f"""
                    SELECT
                        COUNT(*) as table_count,
                        SUM(row_count) as total_rows,
                        SUM(size_bytes) as total_bytes,
                        MIN(refreshed_at) as oldest_refresh,
                        MAX(refreshed_at) as newest_refresh
                    FROM {self.METADATA_TABLE}
                """)
                return dict(cursor.fetchone())
        except Exception as e:
            return {"error": str(e)}
