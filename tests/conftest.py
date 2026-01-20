# -*- coding: utf-8 -*-
"""
Pytest configuration for PyDeequ tests using Spark Connect.

The Spark Connect server is automatically started by the spark_connect_server fixture.
"""

import os
import pytest
from pyspark.sql import SparkSession


# Set environment variables required for pydeequ
os.environ.setdefault("SPARK_VERSION", "3.5")


@pytest.fixture(scope="session")
def spark_connect_server():
    """Session-scoped fixture to start Spark Connect server.

    Automatically starts the Spark Connect server if not already running,
    using the benchmark configuration. The server is NOT stopped after
    tests complete (to allow reuse across test runs).
    """
    from benchmark.spark_server import SparkConnectServer
    from benchmark.config import SparkServerConfig

    config = SparkServerConfig()
    server = SparkConnectServer(config)

    if not server.is_running():
        print("\nStarting Spark Connect server for tests...")
        server.start()
        print("Spark Connect server started.")
    else:
        print("\nSpark Connect server already running.")

    # Set SPARK_REMOTE if not already set
    if not os.environ.get("SPARK_REMOTE"):
        os.environ["SPARK_REMOTE"] = f"sc://localhost:{config.port}"

    yield server
    # Note: We don't stop the server here to allow reuse across test runs


def create_spark_connect_session() -> SparkSession:
    """
    Create a Spark Connect session for testing.

    Requires Spark Connect server to be running on localhost:15002.
    Start the server with the Deequ plugin loaded.

    Returns:
        SparkSession connected to Spark Connect server
    """
    return SparkSession.builder.remote("sc://localhost:15002").getOrCreate()


@pytest.fixture(scope="module")
def spark(spark_connect_server) -> SparkSession:
    """
    Pytest fixture providing a Spark Connect session.

    The session is shared within each test module for efficiency.
    Depends on spark_connect_server to ensure server is running.

    Yields:
        SparkSession for testing
    """
    session = create_spark_connect_session()
    yield session
    session.stop()


# Alias for backward compatibility with existing tests
spark_session = spark


# Legacy function for unittest-based tests
def setup_pyspark():
    """
    Legacy setup function for unittest-based tests.

    Returns a SparkSession builder configured for Spark Connect.
    This is used by existing unittest classes that call setup_pyspark().getOrCreate().
    """

    class SparkConnectBuilder:
        """Builder that creates Spark Connect sessions."""

        def __init__(self):
            self._app_name = "pydeequ-test"

        def appName(self, name):
            self._app_name = name
            return self

        def master(self, master):
            # Ignored - we always use Spark Connect
            return self

        def config(self, key, value):
            # Ignored - Spark Connect doesn't need these configs
            return self

        def getOrCreate(self):
            return create_spark_connect_session()

    return SparkConnectBuilder()
