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
