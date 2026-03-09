#!/usr/bin/env python3
"""
Testing Data Quality at Scale with PyDeequ V2

This example demonstrates the PyDeequ 2.0 API using Spark Connect.
It covers data analysis, constraint verification, column profiling,
and constraint suggestions.

Prerequisites:
1. Start the Spark Connect server with the Deequ plugin:

   $SPARK_HOME/sbin/start-connect-server.sh \
     --packages org.apache.spark:spark-connect_2.12:3.5.0 \
     --jars /path/to/deequ_2.12-2.1.0b-spark-3.5.jar \
     --conf spark.connect.extensions.relation.classes=com.amazon.deequ.connect.DeequRelationPlugin

2. Run this script:
   SPARK_REMOTE=sc://localhost:15002 python data_quality_example_v2.py
"""

import os
from pyspark.sql import SparkSession, Row

import pydeequ

# PyDeequ V2 imports
from pydeequ.v2.analyzers import (
    Size,
    Completeness,
    Distinctness,
    Mean,
    Minimum,
    Maximum,
    StandardDeviation,
    Correlation,
)
from pydeequ.v2.checks import Check, CheckLevel
from pydeequ.v2.verification import AnalysisRunner, VerificationSuite
from pydeequ.v2.predicates import eq, gte, lte, between
from pydeequ.v2.profiles import ColumnProfilerRunner
from pydeequ.v2.suggestions import ConstraintSuggestionRunner, Rules


def create_sample_data(spark: SparkSession):
    """Create a sample product reviews dataset for demonstration."""
    data = [
        # Normal reviews
        Row(review_id="R001", customer_id="C100", product_id="P001",
            marketplace="US", star_rating=5, helpful_votes=10, total_votes=12,
            review_year=2023, product_title="Great Product", insight="Y"),
        Row(review_id="R002", customer_id="C101", product_id="P002",
            marketplace="US", star_rating=4, helpful_votes=8, total_votes=10,
            review_year=2023, product_title="Good Value", insight="Y"),
        Row(review_id="R003", customer_id="C102", product_id="P001",
            marketplace="UK", star_rating=5, helpful_votes=15, total_votes=18,
            review_year=2022, product_title="Great Product", insight="N"),
        Row(review_id="R004", customer_id="C103", product_id="P003",
            marketplace="DE", star_rating=3, helpful_votes=5, total_votes=8,
            review_year=2022, product_title="Decent Item", insight="Y"),
        Row(review_id="R005", customer_id="C104", product_id="P002",
            marketplace="FR", star_rating=4, helpful_votes=12, total_votes=15,
            review_year=2021, product_title="Good Value", insight="N"),
        Row(review_id="R006", customer_id="C105", product_id="P004",
            marketplace="JP", star_rating=5, helpful_votes=20, total_votes=22,
            review_year=2023, product_title="Excellent!", insight="Y"),
        Row(review_id="R007", customer_id="C106", product_id="P001",
            marketplace="US", star_rating=2, helpful_votes=3, total_votes=10,
            review_year=2020, product_title="Great Product", insight="N"),
        Row(review_id="R008", customer_id="C107", product_id="P005",
            marketplace="UK", star_rating=1, helpful_votes=25, total_votes=30,
            review_year=2021, product_title="Disappointing", insight="Y"),
        # Review with missing marketplace (data quality issue)
        Row(review_id="R009", customer_id="C108", product_id="P002",
            marketplace=None, star_rating=4, helpful_votes=7, total_votes=9,
            review_year=2023, product_title="Good Value", insight="Y"),
        # Duplicate review_id (data quality issue)
        Row(review_id="R001", customer_id="C109", product_id="P003",
            marketplace="US", star_rating=3, helpful_votes=4, total_votes=6,
            review_year=2022, product_title="Decent Item", insight="N"),
    ]
    return spark.createDataFrame(data)


def run_data_analysis(engine, df):
    """
    Run data analysis using AnalysisRunner.

    This demonstrates computing various metrics on the dataset:
    - Size: Total row count
    - Completeness: Ratio of non-null values
    - Distinctness: Ratio of distinct values
    - Mean, Min, Max: Statistical measures
    - Correlation: Relationship between columns
    """
    print("\n" + "=" * 60)
    print("DATA ANALYSIS")
    print("=" * 60)

    result = (AnalysisRunner(engine)
        .onData(dataframe=df)
        .addAnalyzer(Size())
        .addAnalyzer(Completeness("review_id"))
        .addAnalyzer(Completeness("marketplace"))
        .addAnalyzer(Distinctness("review_id"))
        .addAnalyzer(Mean("star_rating"))
        .addAnalyzer(Minimum("star_rating"))
        .addAnalyzer(Maximum("star_rating"))
        .addAnalyzer(StandardDeviation("star_rating"))
        .addAnalyzer(Correlation("total_votes", "helpful_votes"))
        .run())

    print("\nAnalysis Results:")
    result.show(truncate=False)

    # Extract key insights
    rows = result.collect()
    metrics = {(r["name"], r["instance"]): r["value"] for r in rows}

    print("\nKey Insights:")
    print(f"  - Dataset contains {int(metrics.get(('Size', '*'), 0))} reviews")
    print(f"  - review_id completeness: {metrics.get(('Completeness', 'review_id'), 0):.1%}")
    print(f"  - marketplace completeness: {metrics.get(('Completeness', 'marketplace'), 0):.1%}")
    print(f"  - review_id distinctness: {metrics.get(('Distinctness', 'review_id'), 0):.1%}")
    print(f"  - Average star rating: {metrics.get(('Mean', 'star_rating'), 0):.2f}")
    print(f"  - Star rating range: {metrics.get(('Minimum', 'star_rating'), 0):.0f} - {metrics.get(('Maximum', 'star_rating'), 0):.0f}")

    return result


def run_constraint_verification(engine, df):
    """
    Run constraint verification using VerificationSuite.

    This demonstrates defining and verifying data quality rules:
    - Size checks
    - Completeness checks
    - Uniqueness checks
    - Range checks (min/max)
    - Categorical value checks
    """
    print("\n" + "=" * 60)
    print("CONSTRAINT VERIFICATION")
    print("=" * 60)

    # Define checks using the V2 predicate API
    # Note: In V2, we use predicates like eq(), gte(), between() instead of lambdas
    check = (Check(CheckLevel.Warning, "Product Reviews Quality Check")
        # Size check: at least 5 reviews
        .hasSize(gte(5))
        # Completeness checks
        .isComplete("review_id")
        .isComplete("customer_id")
        .hasCompleteness("marketplace", gte(0.8))  # Allow some missing
        # Uniqueness check
        .isUnique("review_id")
        # Star rating range check
        .hasMin("star_rating", eq(1.0))
        .hasMax("star_rating", eq(5.0))
        .hasMean("star_rating", between(1.0, 5.0))
        # Year range check
        .hasMin("review_year", gte(2015))
        .hasMax("review_year", lte(2025))
        # Categorical check
        .isContainedIn("marketplace", ["US", "UK", "DE", "JP", "FR"])
        .isContainedIn("insight", ["Y", "N"])
    )

    result = (VerificationSuite(engine)
        .onData(dataframe=df)
        .addCheck(check)
        .run())

    print("\nVerification Results:")
    result.show(truncate=False)

    # Summarize results
    rows = result.collect()
    passed = sum(1 for r in rows if r["constraint_status"] == "Success")
    failed = sum(1 for r in rows if r["constraint_status"] == "Failure")

    print(f"\nSummary: {passed} passed, {failed} failed out of {len(rows)} constraints")

    if failed > 0:
        print("\nFailed Constraints:")
        for r in rows:
            if r["constraint_status"] == "Failure":
                print(f"  - {r['constraint']}")
                if r["constraint_message"]:
                    print(f"    Message: {r['constraint_message']}")

    return result


def run_column_profiling(engine, df):
    """
    Run column profiling using ColumnProfilerRunner.

    This automatically computes statistics for each column:
    - Completeness
    - Approximate distinct values
    - Data type detection
    - Numeric statistics (mean, min, max, etc.)
    - Optional: KLL sketches for approximate quantiles
    """
    print("\n" + "=" * 60)
    print("COLUMN PROFILING")
    print("=" * 60)

    result = (ColumnProfilerRunner(engine)
        .onData(dataframe=df)
        .withLowCardinalityHistogramThreshold(10)  # Generate histograms for low-cardinality columns
        .run())

    print("\nColumn Profiles:")
    # Show selected columns for readability
    result.select(
        "column", "completeness", "approx_distinct_values",
        "data_type", "mean", "minimum", "maximum"
    ).show(truncate=False)

    return result


def run_constraint_suggestions(engine, df):
    """
    Run automated constraint suggestion using ConstraintSuggestionRunner.

    This analyzes the data and suggests appropriate constraints:
    - Completeness constraints for complete columns
    - Uniqueness constraints for unique columns
    - Categorical range constraints for low-cardinality columns
    - Non-negative constraints for numeric columns
    """
    print("\n" + "=" * 60)
    print("CONSTRAINT SUGGESTIONS")
    print("=" * 60)

    result = (ConstraintSuggestionRunner(engine)
        .onData(dataframe=df)
        .addConstraintRules(Rules.DEFAULT)
        .run())

    print("\nSuggested Constraints:")
    result.select(
        "column_name", "constraint_name", "description", "code_for_constraint"
    ).show(truncate=False)

    # Count suggestions per column
    rows = result.collect()
    print(f"\nTotal suggestions: {len(rows)}")

    return result


def main():
    # Get Spark Connect URL from environment
    spark_remote = os.environ.get("SPARK_REMOTE", "sc://localhost:15002")

    print("PyDeequ V2 Data Quality Example")
    print(f"Connecting to: {spark_remote}")

    # Create Spark Connect session
    spark = SparkSession.builder.remote(spark_remote).getOrCreate()

    try:
        # Create engine using pydeequ.connect()
        engine = pydeequ.connect(spark)

        # Create sample data
        print("\nCreating sample product reviews dataset...")
        df = create_sample_data(spark)

        print("\nDataset Schema:")
        df.printSchema()

        print("\nSample Data:")
        df.show(truncate=False)

        # Run all examples
        run_data_analysis(engine, df)
        run_constraint_verification(engine, df)
        run_column_profiling(engine, df)
        run_constraint_suggestions(engine, df)

        print("\n" + "=" * 60)
        print("EXAMPLE COMPLETE")
        print("=" * 60)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
