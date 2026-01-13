# -*- coding: utf-8 -*-
"""
Pytest configuration for PyDeequ v2 tests using Spark Connect.

Requirements:
- Spark Connect server running on localhost:15002
- Deequ plugin loaded on the server

Start server with:
    $SPARK_HOME/sbin/start-connect-server.sh \
        --jars /path/to/deequ-2.0.9-spark-3.5.jar \
        --conf spark.connect.extensions.relation.classes=com.amazon.deequ.connect.DeequRelationPlugin
"""

import pytest
from pyspark.sql import Row, SparkSession

@pytest.fixture(scope="session")
def spark():
    """
    Session-scoped Spark Connect session.
    Shared across all tests for efficiency.
    """
    session = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
    yield session
    session.stop()


@pytest.fixture(scope="module")
def sample_df(spark):
    """
    Sample DataFrame used across multiple tests.

    Schema:
    - a: string (complete)
    - b: int (complete, unique: 1,2,3)
    - c: int (has null)
    - d: int (all same value: 5)
    """
    return spark.createDataFrame(
        [
            Row(a="foo", b=1, c=5, d=5),
            Row(a="bar", b=2, c=6, d=5),
            Row(a="baz", b=3, c=None, d=5),
        ]
    )


@pytest.fixture(scope="module")
def extended_df(spark):
    """
    Extended DataFrame with more columns for comprehensive tests.
    """
    return spark.createDataFrame(
        [
            Row(
                a="foo",
                b=1,
                c=5,
                d=5,
                e=3,
                f=1,
                g="a",
                email="foo@example.com",
                creditCard="5130566665286573",
            ),
            Row(
                a="bar",
                b=2,
                c=6,
                d=5,
                e=2,
                f=2,
                g="b",
                email="bar@example.com",
                creditCard="4532677117740914",
            ),
            Row(
                a="baz",
                b=3,
                c=None,
                d=5,
                e=1,
                f=1,
                g=None,
                email="baz@example.com",
                creditCard="340145324521741",
            ),
        ]
    )


@pytest.fixture(scope="module")
def profiler_df(spark):
    """
    DataFrame with varied data types for Column Profiler testing.

    Schema:
    - id: int (complete, unique)
    - name: string (complete)
    - age: int (has 1 null)
    - salary: double (has 1 null)
    - active: boolean (complete)
    - email: string (has 1 null)
    - score: double (has 1 null)
    """
    return spark.createDataFrame(
        [
            Row(id=1, name="Alice", age=30, salary=75000.0, active=True,
                email="alice@example.com", score=85.5),
            Row(id=2, name="Bob", age=25, salary=65000.0, active=True,
                email="bob@example.com", score=92.0),
            Row(id=3, name="Charlie", age=35, salary=None, active=False,
                email=None, score=78.5),
            Row(id=4, name="Diana", age=28, salary=80000.0, active=True,
                email="diana@example.com", score=95.0),
            Row(id=5, name="Eve", age=None, salary=70000.0, active=True,
                email="eve@example.com", score=None),
            Row(id=6, name="Frank", age=45, salary=95000.0, active=True,
                email="frank@example.com", score=88.0),
            Row(id=7, name="Grace", age=32, salary=72000.0, active=False,
                email="grace@example.com", score=91.5),
            Row(id=8, name="Henry", age=29, salary=68000.0, active=True,
                email="henry@example.com", score=82.0),
        ]
    )


@pytest.fixture(scope="module")
def suggestion_df(spark):
    """
    DataFrame designed to trigger specific constraint suggestions.

    Characteristics:
    - id: complete and unique -> should suggest NotNull + Unique
    - status: categorical (3 values) -> should suggest IsIn
    - score: numeric range -> should suggest Min/Max
    - category: categorical (3 values) -> should suggest IsIn
    """
    return spark.createDataFrame(
        [
            Row(id=1, status="active", score=85, category="A"),
            Row(id=2, status="active", score=92, category="B"),
            Row(id=3, status="inactive", score=78, category="A"),
            Row(id=4, status="active", score=95, category="C"),
            Row(id=5, status="pending", score=88, category="B"),
            Row(id=6, status="active", score=91, category="A"),
            Row(id=7, status="inactive", score=82, category="C"),
            Row(id=8, status="active", score=89, category="B"),
        ]
    )


@pytest.fixture(scope="module")
def e2e_df(spark):
    """
    DataFrame for end-to-end testing with realistic data.

    Characteristics:
    - Mixed data types (int, string, double)
    - Some null values
    - Valid email patterns
    - Range of numeric values
    """
    return spark.createDataFrame(
        [
            Row(id=1, name="Alice", email="alice@example.com", age=30, score=85.5),
            Row(id=2, name="Bob", email="bob@example.com", age=25, score=92.0),
            Row(id=3, name="Charlie", email=None, age=35, score=78.5),
            Row(id=4, name="Diana", email="diana@example.com", age=28, score=95.0),
            Row(id=5, name="Eve", email="eve@example.com", age=None, score=88.0),
        ]
    )
