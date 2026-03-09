# -*- coding: utf-8 -*-
"""
DuckDB engine configuration for PyDeequ.

This module provides configuration options to optimize DuckDB performance
for analytical workloads like data quality checks.

Example usage:
    import duckdb
    from pydeequ.engines.duckdb import DuckDBEngine
    from pydeequ.engines.duckdb_config import DuckDBEngineConfig

    # Create config with optimizations
    config = DuckDBEngineConfig(
        threads=8,
        memory_limit="8GB",
        preserve_insertion_order=False,  # Better parallelism
    )

    con = duckdb.connect()
    config.apply(con)

    engine = DuckDBEngine(con, table="test")
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, Optional

if TYPE_CHECKING:
    import duckdb


@dataclass
class DuckDBEngineConfig:
    """
    Configuration for DuckDB engine optimization.

    Attributes:
        threads: Number of threads to use. None = auto (all cores).
        memory_limit: Memory limit string (e.g., "8GB"). None = auto.
        preserve_insertion_order: If False, allows better parallelism.
            Set to False for read-only analytical workloads.
        parquet_metadata_cache: Cache Parquet metadata for faster repeated reads.
        enable_object_cache: Enable object caching for repeated queries.
        enable_progress_bar: Show progress bar for long-running queries.
        default_null_order: How to order NULLs (NULLS_FIRST, NULLS_LAST).
        custom_settings: Additional DuckDB settings as key-value pairs.
    """

    threads: Optional[int] = None
    memory_limit: Optional[str] = None
    preserve_insertion_order: bool = False
    parquet_metadata_cache: bool = True
    enable_object_cache: bool = True
    enable_progress_bar: bool = False
    default_null_order: str = "NULLS_LAST"
    custom_settings: Dict[str, str] = field(default_factory=dict)

    def apply(self, con: "duckdb.DuckDBPyConnection") -> None:
        """
        Apply configuration settings to a DuckDB connection.

        Args:
            con: DuckDB connection to configure
        """
        # Thread configuration
        if self.threads is not None:
            con.execute(f"SET threads = {self.threads}")

        # Memory configuration
        if self.memory_limit is not None:
            con.execute(f"SET memory_limit = '{self.memory_limit}'")

        # Parallelism optimization
        con.execute(
            f"SET preserve_insertion_order = {str(self.preserve_insertion_order).lower()}"
        )

        # Caching optimization
        if self.parquet_metadata_cache:
            con.execute("SET parquet_metadata_cache = true")

        if self.enable_object_cache:
            con.execute("SET enable_object_cache = true")

        # Progress bar (useful for debugging long queries)
        con.execute(
            f"SET enable_progress_bar = {str(self.enable_progress_bar).lower()}"
        )

        # NULL ordering
        con.execute(f"SET default_null_order = '{self.default_null_order}'")

        # Apply custom settings
        for key, value in self.custom_settings.items():
            # Determine if value needs quoting (strings vs numbers/booleans)
            if value.lower() in ("true", "false") or value.isdigit():
                con.execute(f"SET {key} = {value}")
            else:
                con.execute(f"SET {key} = '{value}'")

    @classmethod
    def default(cls) -> "DuckDBEngineConfig":
        """Create a default configuration."""
        return cls()

    @classmethod
    def high_performance(cls) -> "DuckDBEngineConfig":
        """
        Create a high-performance configuration for analytical workloads.

        This configuration prioritizes read performance over write safety:
        - Disables insertion order preservation for better parallelism
        - Enables all caching options
        - Uses all available cores
        """
        return cls(
            threads=None,  # Use all cores
            preserve_insertion_order=False,
            parquet_metadata_cache=True,
            enable_object_cache=True,
        )

    @classmethod
    def memory_constrained(cls, memory_limit: str = "4GB") -> "DuckDBEngineConfig":
        """
        Create a configuration for memory-constrained environments.

        Args:
            memory_limit: Memory limit string (e.g., "4GB")
        """
        return cls(
            memory_limit=memory_limit,
            enable_object_cache=False,  # Reduce memory usage
        )


__all__ = ["DuckDBEngineConfig"]
