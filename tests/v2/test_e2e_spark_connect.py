# -*- coding: utf-8 -*-
"""
End-to-End tests for PyDeequ via Spark Connect.

These tests verify that the full Spark Connect pipeline works correctly,
from Python client through the gRPC protocol to the Scala DeequRelationPlugin.

Prerequisites:
1. Build the Deequ JAR with Spark Connect plugin:
   cd deequ && mvn package -DskipTests

2. Start Spark Connect server with the plugin:
   ./scripts/start-spark-connect.sh

3. Run these tests:
   SPARK_REMOTE=sc://localhost:15002 pytest tests/test_e2e_spark_connect.py -v

Note: These tests do NOT use Py4J fallback - they test the actual Spark Connect
protocol with the DeequRelationPlugin on the server side.
"""

import os

import pytest
from pyspark.sql import Row, SparkSession

from pydeequ.v2.analyzers import (
    Completeness,
    Distinctness,
    Maximum,
    Mean,
    Minimum,
    Size,
    StandardDeviation,
    Uniqueness,
)
from pydeequ.v2.checks import Check, CheckLevel
from pydeequ.v2.predicates import between, eq, gt, gte, is_one, lt, lte

# Import the new Spark Connect API
from pydeequ.v2.verification import AnalysisRunner, VerificationSuite

# Skip all tests if SPARK_REMOTE is not set
pytestmark = pytest.mark.skipif(
    reason="SPARK_REMOTE environment variable not set. Start Spark Connect server first.",
)


@pytest.fixture(scope="module")
def spark():
    """Create a Spark Connect session."""
    remote_url = os.environ.get("SPARK_REMOTE", "sc://localhost:15002")

    session = SparkSession.builder.remote(remote_url).getOrCreate()

    yield session

    session.stop()


@pytest.fixture(scope="module")
def sample_df(spark):
    """Create a sample DataFrame for testing."""
    data = [
        Row(id=1, name="Alice", email="alice@example.com", age=30, score=85.5),
        Row(id=2, name="Bob", email="bob@example.com", age=25, score=92.0),
        Row(id=3, name="Charlie", email=None, age=35, score=78.5),
        Row(id=4, name="Diana", email="diana@example.com", age=28, score=95.0),
        Row(id=5, name="Eve", email="eve@example.com", age=None, score=88.0),
    ]
    return spark.createDataFrame(data)


class TestVerificationSuiteE2E:
    """End-to-end tests for VerificationSuite via Spark Connect."""

    def test_size_check(self, spark, sample_df):
        """Test that hasSize check works via Spark Connect."""
        check = Check(CheckLevel.Error, "Size check").hasSize(eq(5))

        result = VerificationSuite(spark).onData(sample_df).addCheck(check).run()

        # Result should be a DataFrame
        assert result is not None

        # Collect results
        rows = result.collect()
        assert len(rows) > 0

        # Check should pass (we have exactly 5 rows)
        row = rows[0]
        assert row["constraint_status"] == "Success"

    def test_completeness_check_passing(self, spark, sample_df):
        """Test completeness check that should pass."""
        check = (
            Check(CheckLevel.Error, "Completeness check")
            .isComplete("id")
            .isComplete("name")
        )

        result = VerificationSuite(spark).onData(sample_df).addCheck(check).run()

        rows = result.collect()

        # Both constraints should pass (id and name are complete)
        for row in rows:
            assert row["constraint_status"] == "Success"

    def test_completeness_check_failing(self, spark, sample_df):
        """Test completeness check that should fail."""
        check = Check(CheckLevel.Error, "Completeness check").isComplete(
            "email"
        )  # email has NULL values

        result = VerificationSuite(spark).onData(sample_df).addCheck(check).run()

        rows = result.collect()

        # Should fail because email has NULL
        assert len(rows) > 0
        assert rows[0]["constraint_status"] == "Failure"

    def test_has_completeness_with_threshold(self, spark, sample_df):
        """Test hasCompleteness with a threshold."""
        # email is 80% complete (4 out of 5)
        check = Check(CheckLevel.Warning, "Completeness threshold").hasCompleteness(
            "email", gte(0.8)
        )

        result = VerificationSuite(spark).onData(sample_df).addCheck(check).run()

        rows = result.collect()
        assert len(rows) > 0
        assert rows[0]["constraint_status"] == "Success"

    def test_uniqueness_check(self, spark, sample_df):
        """Test uniqueness check."""
        check = Check(CheckLevel.Error, "Uniqueness check").isUnique("id")

        result = VerificationSuite(spark).onData(sample_df).addCheck(check).run()

        rows = result.collect()
        assert len(rows) > 0
        assert rows[0]["constraint_status"] == "Success"

    def test_mean_check(self, spark, sample_df):
        """Test mean check with range assertion."""
        # Mean age should be around 29.5 (average of 30, 25, 35, 28, NULL)
        check = Check(CheckLevel.Error, "Mean check").hasMean(
            "score", between(80.0, 95.0)
        )

        result = VerificationSuite(spark).onData(sample_df).addCheck(check).run()

        rows = result.collect()
        assert len(rows) > 0
        assert rows[0]["constraint_status"] == "Success"

    def test_multiple_checks(self, spark, sample_df):
        """Test multiple checks in a single verification run."""
        check = (
            Check(CheckLevel.Error, "Multiple checks")
            .hasSize(eq(5))
            .isComplete("id")
            .isComplete("name")
            .isUnique("id")
            .hasCompleteness("email", gte(0.7))
        )

        result = VerificationSuite(spark).onData(sample_df).addCheck(check).run()

        rows = result.collect()

        # All 5 constraints should pass
        assert len(rows) == 5
        for row in rows:
            assert row["constraint_status"] == "Success"

    def test_check_levels(self, spark, sample_df):
        """Test both Error and Warning check levels."""
        error_check = Check(CheckLevel.Error, "Error level check").isComplete("id")

        warning_check = Check(CheckLevel.Warning, "Warning level check").isComplete(
            "email"
        )  # Will fail

        result = (
            VerificationSuite(spark)
            .onData(sample_df)
            .addCheck(error_check)
            .addCheck(warning_check)
            .run()
        )

        rows = result.collect()

        # Find the results for each check
        error_result = [r for r in rows if r["check"] == "Error level check"][0]
        warning_result = [r for r in rows if r["check"] == "Warning level check"][0]

        assert error_result["check_level"] == "Error"
        assert error_result["constraint_status"] == "Success"

        assert warning_result["check_level"] == "Warning"
        assert warning_result["constraint_status"] == "Failure"


class TestAnalysisRunnerE2E:
    """End-to-end tests for AnalysisRunner via Spark Connect."""

    def test_size_analyzer(self, spark, sample_df):
        """Test Size analyzer."""
        result = AnalysisRunner(spark).onData(sample_df).addAnalyzer(Size()).run()

        rows = result.collect()
        assert len(rows) > 0

        # Find the Size metric
        size_row = [r for r in rows if r["name"] == "Size"][0]
        assert float(size_row["value"]) == 5.0

    def test_completeness_analyzer(self, spark, sample_df):
        """Test Completeness analyzer."""
        result = (
            AnalysisRunner(spark)
            .onData(sample_df)
            .addAnalyzer(Completeness("id"))
            .addAnalyzer(Completeness("email"))
            .run()
        )

        rows = result.collect()

        # id should be 100% complete
        id_row = [r for r in rows if r["instance"] == "id"][0]
        assert float(id_row["value"]) == 1.0

        # email should be 80% complete
        email_row = [r for r in rows if r["instance"] == "email"][0]
        assert float(email_row["value"]) == 0.8

    def test_statistical_analyzers(self, spark, sample_df):
        """Test statistical analyzers (Mean, Min, Max, StdDev)."""
        result = (
            AnalysisRunner(spark)
            .onData(sample_df)
            .addAnalyzer(Mean("score"))
            .addAnalyzer(Minimum("score"))
            .addAnalyzer(Maximum("score"))
            .addAnalyzer(StandardDeviation("score"))
            .run()
        )

        rows = result.collect()

        # Extract values by metric name
        metrics = {r["name"]: float(r["value"]) for r in rows}

        # Verify expected ranges
        assert 85.0 <= metrics["Mean"] <= 90.0  # Mean of scores
        assert metrics["Minimum"] == 78.5
        assert metrics["Maximum"] == 95.0
        assert metrics["StandardDeviation"] > 0  # Should have some variance

    def test_multiple_analyzers(self, spark, sample_df):
        """Test running multiple analyzers together."""
        result = (
            AnalysisRunner(spark)
            .onData(sample_df)
            .addAnalyzer(Size())
            .addAnalyzer(Completeness("id"))
            .addAnalyzer(Completeness("email"))
            .addAnalyzer(Mean("age"))
            .addAnalyzer(Mean("score"))
            .run()
        )

        rows = result.collect()

        # Should have results for all analyzers
        assert len(rows) >= 5


class TestEdgeCasesE2E:
    """Test edge cases and error handling."""

    def test_empty_dataframe(self, spark):
        """Test verification on empty DataFrame."""
        empty_df = spark.createDataFrame([], "id: int, name: string")

        check = Check(CheckLevel.Error, "Empty DF check").hasSize(eq(0))

        result = VerificationSuite(spark).onData(empty_df).addCheck(check).run()

        rows = result.collect()
        assert len(rows) > 0
        assert rows[0]["constraint_status"] == "Success"

    def test_all_null_column(self, spark):
        """Test completeness on all-NULL column."""
        data = [Row(id=1, val=None), Row(id=2, val=None)]
        df = spark.createDataFrame(data)

        check = Check(CheckLevel.Error, "Null column check").hasCompleteness(
            "val", eq(0.0)
        )

        result = VerificationSuite(spark).onData(df).addCheck(check).run()

        rows = result.collect()
        assert rows[0]["constraint_status"] == "Success"

    def test_single_row(self, spark):
        """Test verification on single-row DataFrame."""
        data = [Row(id=1, name="Test")]
        df = spark.createDataFrame(data)

        check = (
            Check(CheckLevel.Error, "Single row check")
            .hasSize(eq(1))
            .isComplete("id")
            .isUnique("id")
        )

        result = VerificationSuite(spark).onData(df).addCheck(check).run()

        rows = result.collect()
        for row in rows:
            assert row["constraint_status"] == "Success"


class TestPredicatesE2E:
    """Test various predicates via Spark Connect."""

    def test_eq_predicate(self, spark, sample_df):
        """Test eq() predicate."""
        check = Check(CheckLevel.Error, "EQ test").hasSize(eq(5))

        result = VerificationSuite(spark).onData(sample_df).addCheck(check).run()

        rows = result.collect()
        assert rows[0]["constraint_status"] == "Success"

    def test_gte_predicate(self, spark, sample_df):
        """Test gte() predicate."""
        check = Check(CheckLevel.Error, "GTE test").hasCompleteness("id", gte(1.0))

        result = VerificationSuite(spark).onData(sample_df).addCheck(check).run()

        rows = result.collect()
        assert rows[0]["constraint_status"] == "Success"

    def test_between_predicate(self, spark, sample_df):
        """Test between() predicate."""
        check = Check(CheckLevel.Error, "Between test").hasMean(
            "score", between(80.0, 95.0)
        )

        result = VerificationSuite(spark).onData(sample_df).addCheck(check).run()

        rows = result.collect()
        assert rows[0]["constraint_status"] == "Success"

    def test_lt_predicate(self, spark, sample_df):
        """Test lt() predicate - should fail when condition not met."""
        check = Check(CheckLevel.Error, "LT test").hasSize(
            lt(3)
        )  # We have 5 rows, so this should fail

        result = VerificationSuite(spark).onData(sample_df).addCheck(check).run()

        rows = result.collect()
        assert rows[0]["constraint_status"] == "Failure"


if __name__ == "__main__":
    # Run tests directly
    pytest.main([__file__, "-v"])
