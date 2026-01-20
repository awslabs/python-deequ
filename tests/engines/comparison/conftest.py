# Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Dual-engine test fixtures for cross-engine comparison.

Provides fixtures for creating both Spark and DuckDB engines with
identical data for parity testing. These tests require Spark Connect
to be running and SPARK_REMOTE environment variable to be set.
"""

import os
from dataclasses import dataclass
from typing import Callable, Generator, Optional
import pytest
import duckdb
import pandas as pd

from pydeequ.engines import BaseEngine
from pydeequ.engines.duckdb import DuckDBEngine
from tests.engines.fixtures.datasets import DATASET_FACTORIES


# Check if Spark Connect is available
def spark_available() -> bool:
    """Check if Spark Connect is available via SPARK_REMOTE."""
    return os.environ.get("SPARK_REMOTE") is not None


# Skip marker for tests requiring Spark
requires_spark = pytest.mark.skipif(
    not spark_available(),
    reason="Spark Connect not available (set SPARK_REMOTE)"
)


@dataclass
class DualEngines:
    """Container for both Spark and DuckDB engines with same data."""
    spark_engine: BaseEngine
    duckdb_engine: BaseEngine
    dataset_name: str


@pytest.fixture(scope="module")
def spark_session():
    """Create a module-scoped Spark Connect session.

    Only creates session if SPARK_REMOTE is set.
    """
    if not spark_available():
        pytest.skip("Spark Connect not available")

    from pyspark.sql import SparkSession
    spark_remote = os.environ.get("SPARK_REMOTE")
    spark = SparkSession.builder.remote(spark_remote).getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def duckdb_connection() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Create a module-scoped DuckDB connection."""
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()


@pytest.fixture(scope="function")
def dual_engine_factory(
    spark_session,
    duckdb_connection: duckdb.DuckDBPyConnection
) -> Callable[[str], DualEngines]:
    """Factory fixture to create both Spark and DuckDB engines with same data.

    Usage:
        def test_comparison(dual_engine_factory):
            engines = dual_engine_factory("df_full")
            spark_metrics = engines.spark_engine.compute_metrics([Size()])
            duckdb_metrics = engines.duckdb_engine.compute_metrics([Size()])
            assert_metrics_match(spark_metrics, duckdb_metrics)
    """
    tables_created = []

    def factory(dataset_name: str) -> DualEngines:
        if dataset_name not in DATASET_FACTORIES:
            raise ValueError(f"Unknown dataset: {dataset_name}")

        # Get the pandas DataFrame
        pdf = DATASET_FACTORIES[dataset_name]()
        table_name = f"test_{dataset_name}"

        # Create DuckDB engine
        try:
            duckdb_connection.unregister(table_name)
        except Exception:
            pass
        duckdb_connection.register(table_name, pdf)
        duckdb_engine = DuckDBEngine(duckdb_connection, table_name)
        tables_created.append(table_name)

        # Create Spark engine
        from pydeequ.engines.spark import SparkEngine
        spark_df = spark_session.createDataFrame(pdf)
        spark_engine = SparkEngine(spark_session, dataframe=spark_df)

        return DualEngines(
            spark_engine=spark_engine,
            duckdb_engine=duckdb_engine,
            dataset_name=dataset_name
        )

    yield factory

    # Cleanup DuckDB tables
    for table_name in tables_created:
        try:
            duckdb_connection.unregister(table_name)
        except Exception:
            pass


# Convenience fixtures for common datasets


@pytest.fixture(scope="function")
def dual_engines_full(dual_engine_factory) -> DualEngines:
    """Dual engines with df_full dataset."""
    return dual_engine_factory("df_full")


@pytest.fixture(scope="function")
def dual_engines_missing(dual_engine_factory) -> DualEngines:
    """Dual engines with df_missing dataset."""
    return dual_engine_factory("df_missing")


@pytest.fixture(scope="function")
def dual_engines_numeric(dual_engine_factory) -> DualEngines:
    """Dual engines with df_numeric dataset."""
    return dual_engine_factory("df_numeric")


@pytest.fixture(scope="function")
def dual_engines_unique(dual_engine_factory) -> DualEngines:
    """Dual engines with df_unique dataset."""
    return dual_engine_factory("df_unique")


@pytest.fixture(scope="function")
def dual_engines_distinct(dual_engine_factory) -> DualEngines:
    """Dual engines with df_distinct dataset."""
    return dual_engine_factory("df_distinct")


@pytest.fixture(scope="function")
def dual_engines_string_lengths(dual_engine_factory) -> DualEngines:
    """Dual engines with df_string_lengths dataset."""
    return dual_engine_factory("df_string_lengths")


@pytest.fixture(scope="function")
def dual_engines_correlation(dual_engine_factory) -> DualEngines:
    """Dual engines with df_correlation dataset."""
    return dual_engine_factory("df_correlation")


@pytest.fixture(scope="function")
def dual_engines_entropy(dual_engine_factory) -> DualEngines:
    """Dual engines with df_entropy dataset."""
    return dual_engine_factory("df_entropy")


@pytest.fixture(scope="function")
def dual_engines_compliance(dual_engine_factory) -> DualEngines:
    """Dual engines with df_compliance dataset."""
    return dual_engine_factory("df_compliance")


@pytest.fixture(scope="function")
def dual_engines_pattern(dual_engine_factory) -> DualEngines:
    """Dual engines with df_pattern dataset."""
    return dual_engine_factory("df_pattern")


@pytest.fixture(scope="function")
def dual_engines_quantile(dual_engine_factory) -> DualEngines:
    """Dual engines with df_quantile dataset."""
    return dual_engine_factory("df_quantile")


@pytest.fixture(scope="function")
def dual_engines_contained_in(dual_engine_factory) -> DualEngines:
    """Dual engines with df_contained_in dataset."""
    return dual_engine_factory("df_contained_in")


@pytest.fixture(scope="function")
def dual_engines_histogram(dual_engine_factory) -> DualEngines:
    """Dual engines with df_histogram dataset."""
    return dual_engine_factory("df_histogram")


@pytest.fixture(scope="function")
def dual_engines_mutual_info(dual_engine_factory) -> DualEngines:
    """Dual engines with df_mutual_info dataset."""
    return dual_engine_factory("df_mutual_info")


@pytest.fixture(scope="function")
def dual_engines_where(dual_engine_factory) -> DualEngines:
    """Dual engines with df_where dataset."""
    return dual_engine_factory("df_where")


@pytest.fixture(scope="function")
def dual_engines_all_null(dual_engine_factory) -> DualEngines:
    """Dual engines with df_all_null dataset."""
    return dual_engine_factory("df_all_null")


@pytest.fixture(scope="function")
def dual_engines_single(dual_engine_factory) -> DualEngines:
    """Dual engines with df_single dataset."""
    return dual_engine_factory("df_single")


@pytest.fixture(scope="function")
def dual_engines_empty(dual_engine_factory) -> DualEngines:
    """Dual engines with df_empty dataset."""
    return dual_engine_factory("df_empty")
