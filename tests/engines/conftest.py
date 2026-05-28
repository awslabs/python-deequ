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

"""DuckDB engine test fixtures.

Provides fixtures for creating DuckDB engines with various test datasets.
These fixtures are used by DuckDB-only tests that don't require Spark.
"""

from typing import Callable, Generator
import pytest
import duckdb
import pandas as pd

from pydeequ.engines.duckdb import DuckDBEngine
from tests.engines.fixtures.datasets import (
    create_df_full,
    create_df_missing,
    create_df_numeric,
    create_df_unique,
    create_df_distinct,
    create_df_string_lengths,
    create_df_empty,
    create_df_single,
    create_df_all_null,
    create_df_escape,
    create_df_correlation,
    create_df_entropy,
    create_df_where,
    create_df_pattern,
    create_df_compliance,
    create_df_quantile,
    create_df_contained_in,
    create_df_histogram,
    create_df_mutual_info,
    create_df_data_type,
    DATASET_FACTORIES,
)


@pytest.fixture(scope="module")
def duckdb_connection() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Create a module-scoped DuckDB connection."""
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()


def _create_engine_from_df(
    conn: duckdb.DuckDBPyConnection,
    df: pd.DataFrame,
    table_name: str
) -> DuckDBEngine:
    """Helper to create a DuckDB engine from a pandas DataFrame."""
    # Register the DataFrame as a table
    conn.register(table_name, df)
    # Create engine pointing to the table
    return DuckDBEngine(conn, table_name)


@pytest.fixture(scope="function")
def engine_factory(duckdb_connection: duckdb.DuckDBPyConnection) -> Callable[[str], DuckDBEngine]:
    """Factory fixture to create DuckDB engines for any dataset.

    Usage:
        def test_something(engine_factory):
            engine = engine_factory("df_full")
            results = engine.compute_metrics([Size()])
    """
    tables_created = []

    def factory(dataset_name: str) -> DuckDBEngine:
        if dataset_name not in DATASET_FACTORIES:
            raise ValueError(f"Unknown dataset: {dataset_name}")

        table_name = f"test_{dataset_name}"
        df = DATASET_FACTORIES[dataset_name]()

        # Unregister if already exists (for reuse in same test)
        try:
            duckdb_connection.unregister(table_name)
        except Exception:
            pass

        duckdb_connection.register(table_name, df)
        tables_created.append(table_name)

        return DuckDBEngine(duckdb_connection, table_name)

    yield factory

    # Cleanup: unregister all tables
    for table_name in tables_created:
        try:
            duckdb_connection.unregister(table_name)
        except Exception:
            pass


# Individual dataset fixtures for convenience


@pytest.fixture(scope="function")
def engine_full(duckdb_connection: duckdb.DuckDBPyConnection) -> Generator[DuckDBEngine, None, None]:
    """DuckDB engine with df_full dataset."""
    table_name = "test_df_full"
    df = create_df_full()
    duckdb_connection.register(table_name, df)
    yield DuckDBEngine(duckdb_connection, table_name)
    duckdb_connection.unregister(table_name)


@pytest.fixture(scope="function")
def engine_missing(duckdb_connection: duckdb.DuckDBPyConnection) -> Generator[DuckDBEngine, None, None]:
    """DuckDB engine with df_missing dataset."""
    table_name = "test_df_missing"
    df = create_df_missing()
    duckdb_connection.register(table_name, df)
    yield DuckDBEngine(duckdb_connection, table_name)
    duckdb_connection.unregister(table_name)


@pytest.fixture(scope="function")
def engine_numeric(duckdb_connection: duckdb.DuckDBPyConnection) -> Generator[DuckDBEngine, None, None]:
    """DuckDB engine with df_numeric dataset."""
    table_name = "test_df_numeric"
    df = create_df_numeric()
    duckdb_connection.register(table_name, df)
    yield DuckDBEngine(duckdb_connection, table_name)
    duckdb_connection.unregister(table_name)


@pytest.fixture(scope="function")
def engine_unique(duckdb_connection: duckdb.DuckDBPyConnection) -> Generator[DuckDBEngine, None, None]:
    """DuckDB engine with df_unique dataset."""
    table_name = "test_df_unique"
    df = create_df_unique()
    duckdb_connection.register(table_name, df)
    yield DuckDBEngine(duckdb_connection, table_name)
    duckdb_connection.unregister(table_name)


@pytest.fixture(scope="function")
def engine_distinct(duckdb_connection: duckdb.DuckDBPyConnection) -> Generator[DuckDBEngine, None, None]:
    """DuckDB engine with df_distinct dataset."""
    table_name = "test_df_distinct"
    df = create_df_distinct()
    duckdb_connection.register(table_name, df)
    yield DuckDBEngine(duckdb_connection, table_name)
    duckdb_connection.unregister(table_name)


@pytest.fixture(scope="function")
def engine_string_lengths(duckdb_connection: duckdb.DuckDBPyConnection) -> Generator[DuckDBEngine, None, None]:
    """DuckDB engine with df_string_lengths dataset."""
    table_name = "test_df_string_lengths"
    df = create_df_string_lengths()
    duckdb_connection.register(table_name, df)
    yield DuckDBEngine(duckdb_connection, table_name)
    duckdb_connection.unregister(table_name)


@pytest.fixture(scope="function")
def engine_empty(duckdb_connection: duckdb.DuckDBPyConnection) -> Generator[DuckDBEngine, None, None]:
    """DuckDB engine with df_empty dataset."""
    table_name = "test_df_empty"
    df = create_df_empty()
    duckdb_connection.register(table_name, df)
    yield DuckDBEngine(duckdb_connection, table_name)
    duckdb_connection.unregister(table_name)


@pytest.fixture(scope="function")
def engine_single(duckdb_connection: duckdb.DuckDBPyConnection) -> Generator[DuckDBEngine, None, None]:
    """DuckDB engine with df_single dataset."""
    table_name = "test_df_single"
    df = create_df_single()
    duckdb_connection.register(table_name, df)
    yield DuckDBEngine(duckdb_connection, table_name)
    duckdb_connection.unregister(table_name)


@pytest.fixture(scope="function")
def engine_all_null(duckdb_connection: duckdb.DuckDBPyConnection) -> Generator[DuckDBEngine, None, None]:
    """DuckDB engine with df_all_null dataset."""
    table_name = "test_df_all_null"
    df = create_df_all_null()
    duckdb_connection.register(table_name, df)
    yield DuckDBEngine(duckdb_connection, table_name)
    duckdb_connection.unregister(table_name)


@pytest.fixture(scope="function")
def engine_escape(duckdb_connection: duckdb.DuckDBPyConnection) -> Generator[DuckDBEngine, None, None]:
    """DuckDB engine with df_escape dataset."""
    table_name = "test_df_escape"
    df = create_df_escape()
    duckdb_connection.register(table_name, df)
    yield DuckDBEngine(duckdb_connection, table_name)
    duckdb_connection.unregister(table_name)


@pytest.fixture(scope="function")
def engine_correlation(duckdb_connection: duckdb.DuckDBPyConnection) -> Generator[DuckDBEngine, None, None]:
    """DuckDB engine with df_correlation dataset."""
    table_name = "test_df_correlation"
    df = create_df_correlation()
    duckdb_connection.register(table_name, df)
    yield DuckDBEngine(duckdb_connection, table_name)
    duckdb_connection.unregister(table_name)


@pytest.fixture(scope="function")
def engine_entropy(duckdb_connection: duckdb.DuckDBPyConnection) -> Generator[DuckDBEngine, None, None]:
    """DuckDB engine with df_entropy dataset."""
    table_name = "test_df_entropy"
    df = create_df_entropy()
    duckdb_connection.register(table_name, df)
    yield DuckDBEngine(duckdb_connection, table_name)
    duckdb_connection.unregister(table_name)


@pytest.fixture(scope="function")
def engine_where(duckdb_connection: duckdb.DuckDBPyConnection) -> Generator[DuckDBEngine, None, None]:
    """DuckDB engine with df_where dataset."""
    table_name = "test_df_where"
    df = create_df_where()
    duckdb_connection.register(table_name, df)
    yield DuckDBEngine(duckdb_connection, table_name)
    duckdb_connection.unregister(table_name)


@pytest.fixture(scope="function")
def engine_pattern(duckdb_connection: duckdb.DuckDBPyConnection) -> Generator[DuckDBEngine, None, None]:
    """DuckDB engine with df_pattern dataset."""
    table_name = "test_df_pattern"
    df = create_df_pattern()
    duckdb_connection.register(table_name, df)
    yield DuckDBEngine(duckdb_connection, table_name)
    duckdb_connection.unregister(table_name)


@pytest.fixture(scope="function")
def engine_compliance(duckdb_connection: duckdb.DuckDBPyConnection) -> Generator[DuckDBEngine, None, None]:
    """DuckDB engine with df_compliance dataset."""
    table_name = "test_df_compliance"
    df = create_df_compliance()
    duckdb_connection.register(table_name, df)
    yield DuckDBEngine(duckdb_connection, table_name)
    duckdb_connection.unregister(table_name)


@pytest.fixture(scope="function")
def engine_quantile(duckdb_connection: duckdb.DuckDBPyConnection) -> Generator[DuckDBEngine, None, None]:
    """DuckDB engine with df_quantile dataset."""
    table_name = "test_df_quantile"
    df = create_df_quantile()
    duckdb_connection.register(table_name, df)
    yield DuckDBEngine(duckdb_connection, table_name)
    duckdb_connection.unregister(table_name)


@pytest.fixture(scope="function")
def engine_contained_in(duckdb_connection: duckdb.DuckDBPyConnection) -> Generator[DuckDBEngine, None, None]:
    """DuckDB engine with df_contained_in dataset."""
    table_name = "test_df_contained_in"
    df = create_df_contained_in()
    duckdb_connection.register(table_name, df)
    yield DuckDBEngine(duckdb_connection, table_name)
    duckdb_connection.unregister(table_name)


@pytest.fixture(scope="function")
def engine_histogram(duckdb_connection: duckdb.DuckDBPyConnection) -> Generator[DuckDBEngine, None, None]:
    """DuckDB engine with df_histogram dataset."""
    table_name = "test_df_histogram"
    df = create_df_histogram()
    duckdb_connection.register(table_name, df)
    yield DuckDBEngine(duckdb_connection, table_name)
    duckdb_connection.unregister(table_name)


@pytest.fixture(scope="function")
def engine_mutual_info(duckdb_connection: duckdb.DuckDBPyConnection) -> Generator[DuckDBEngine, None, None]:
    """DuckDB engine with df_mutual_info dataset."""
    table_name = "test_df_mutual_info"
    df = create_df_mutual_info()
    duckdb_connection.register(table_name, df)
    yield DuckDBEngine(duckdb_connection, table_name)
    duckdb_connection.unregister(table_name)


@pytest.fixture(scope="function")
def engine_data_type(duckdb_connection: duckdb.DuckDBPyConnection) -> Generator[DuckDBEngine, None, None]:
    """DuckDB engine with df_data_type dataset."""
    table_name = "test_df_data_type"
    df = create_df_data_type()
    duckdb_connection.register(table_name, df)
    yield DuckDBEngine(duckdb_connection, table_name)
    duckdb_connection.unregister(table_name)


# Helper function for metric lookup
def get_metric_value(metrics, name: str, instance: str = None) -> float:
    """Extract a metric value from results by name and optionally instance."""
    for m in metrics:
        if m.name == name:
            if instance is None or m.instance == instance:
                return m.value
    return None


def get_metric(metrics, name: str, instance: str = None):
    """Extract a metric result from results by name and optionally instance."""
    for m in metrics:
        if m.name == name:
            if instance is None or m.instance == instance:
                return m
    return None
