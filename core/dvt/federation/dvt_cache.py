"""
DVT Cache — persistent DuckDB database for extraction model data.

Lives at .dvt/cache.duckdb in the project directory. Persists between runs.

Contains:
- Cached source tables: {source_name}__{table_name} (extracted via Sling)
- Model result tables: {model_name} (for incremental {{ this }} support)

Cache lifecycle:
- Created on first dvt run that needs extraction
- Persists between runs
- --full-refresh deletes the cache file
- dvt clean deletes the .dvt/ directory
"""

import logging
import os
import shutil
import threading
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

DEFAULT_CACHE_DIR = ".dvt"
DEFAULT_CACHE_FILE = "cache.duckdb"


class DvtCache:
    """Persistent DuckDB cache for extraction models.

    Manages the .dvt/cache.duckdb file in the project directory.
    Sources are cached per-source (shared across models).
    Model results are cached for incremental {{ this }} support.
    """

    # Class-level lock — ensures only one model at a time uses the cache file.
    # DuckDB has exclusive file locking. Sling is an external process that also
    # needs the lock. This serializes: open→Sling→close→next model.
    _file_lock = threading.Lock()

    def __init__(
        self,
        project_dir: str = ".",
        cache_dir: Optional[str] = None,
        memory_limit: str = "4GB",
        threads: int = 4,
    ) -> None:
        self._project_dir = os.path.abspath(project_dir)
        self._cache_dir_name = cache_dir or DEFAULT_CACHE_DIR
        self._memory_limit = memory_limit
        self._threads = threads
        self._conn = None

    @property
    def cache_dir(self) -> str:
        """Full path to the .dvt/ cache directory."""
        return os.path.join(self._project_dir, self._cache_dir_name)

    @property
    def db_path(self) -> str:
        """Full path to the cache.duckdb file."""
        return os.path.join(self.cache_dir, DEFAULT_CACHE_FILE)

    @property
    def conn(self):
        """Lazy-initialize the DuckDB connection to the cache file."""
        if self._conn is None:
            try:
                import duckdb
            except ImportError:
                raise RuntimeError(
                    "DuckDB is required for extraction models but is not installed. "
                    "Run 'dvt sync' to install dependencies."
                )

            # Ensure .dvt/ directory exists
            os.makedirs(self.cache_dir, exist_ok=True)

            self._conn = duckdb.connect(self.db_path)
            self._conn.execute(f"SET memory_limit = '{self._memory_limit}'")
            self._conn.execute(f"SET threads = {self._threads}")

            logger.info(
                f"DVT cache opened: {self.db_path} "
                f"(memory={self._memory_limit}, threads={self._threads})"
            )
        return self._conn

    # ------------------------------------------------------------------
    # Source table management
    # ------------------------------------------------------------------

    def source_table_name(self, source_name: str, table_name: str) -> str:
        """Get the DuckDB table name for a cached source."""
        return f"{source_name}__{table_name}"

    def has_source_table(self, source_name: str, table_name: str) -> bool:
        """Check if a source table exists in the cache."""
        tbl = self.source_table_name(source_name, table_name)
        return self._table_exists(tbl)

    # ------------------------------------------------------------------
    # Model result management (for incremental {{ this }})
    # ------------------------------------------------------------------

    def model_table_name(self, model_name: str) -> str:
        """Get the DuckDB table name for a cached model result."""
        return f"__model__{model_name}"

    def has_model_result(self, model_name: str) -> bool:
        """Check if a model result exists in the cache (for is_incremental)."""
        tbl = self.model_table_name(model_name)
        return self._table_exists(tbl)

    def save_model_result(self, model_name: str, sql: str) -> int:
        """Execute model SQL and save the result as a cached table.

        Returns row count.
        """
        tbl = self.model_table_name(model_name)
        self.conn.execute(f"CREATE OR REPLACE TABLE {tbl} AS ({sql})")
        result = self.conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()
        row_count = result[0] if result else 0
        logger.info(f"DVT cache: model result '{tbl}' saved ({row_count} rows)")
        return row_count

    # ------------------------------------------------------------------
    # SQL execution
    # ------------------------------------------------------------------

    def execute(self, sql: str) -> Any:
        """Execute SQL in the cache DuckDB."""
        return self.conn.execute(sql)

    def execute_fetchone(self, sql: str) -> Optional[tuple]:
        """Execute SQL and fetch one row."""
        result = self.conn.execute(sql)
        return result.fetchone()

    # ------------------------------------------------------------------
    # Table utilities
    # ------------------------------------------------------------------

    def list_tables(self) -> List[str]:
        """List all tables in the cache."""
        result = self.conn.execute("SHOW TABLES").fetchall()
        return [row[0] for row in result]

    def drop_table(self, table_name: str) -> None:
        """Drop a table from the cache."""
        self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")

    def _table_exists(self, table_name: str) -> bool:
        """Check if a table exists in DuckDB."""
        try:
            result = self.conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                [table_name],
            ).fetchone()
            return result is not None and result[0] > 0
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Cache lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close the DuckDB connection (but keep the file)."""
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
            logger.info("DVT cache: connection closed")

    def ensure_created(self) -> None:
        """Ensure the cache file exists (creates it if needed)."""
        # Accessing self.conn triggers lazy creation
        _ = self.conn
        logger.info(f"DVT cache: ensured at {self.db_path}")

    def close_and_release(self) -> None:
        """Close connection and release file lock so Sling can access it."""
        self.close()

    def reopen(self) -> None:
        """Reopen the connection after Sling is done."""
        # Just reset _conn — the property will lazy-init on next access
        self._conn = None

    def destroy(self) -> None:
        """Delete the entire cache (for --full-refresh and dvt clean)."""
        self.close()
        if os.path.exists(self.cache_dir):
            shutil.rmtree(self.cache_dir, ignore_errors=True)
            logger.info(f"DVT cache: destroyed {self.cache_dir}")

    def destroy_tables(self) -> None:
        """Drop all tables but keep the file (lighter than full destroy)."""
        tables = self.list_tables()
        for tbl in tables:
            self.drop_table(tbl)
        logger.info(f"DVT cache: dropped {len(tables)} tables")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
