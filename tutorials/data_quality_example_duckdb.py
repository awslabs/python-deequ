#!/usr/bin/env python3
"""
Testing Data Quality at Scale with PyDeequ + DuckDB

This example demonstrates using PyDeequ with DuckDB as the execution backend,
enabling data quality checks without a Spark cluster.

It covers:
- Data analysis (AnalysisRunner)
- Constraint verification (VerificationSuite)
- Column profiling (ColumnProfilerRunner)
- Constraint suggestions (ConstraintSuggestionRunner)

Prerequisites:
1. Install dependencies:
   pip install duckdb pandas

2. Run this script:
   python data_quality_example_duckdb.py
"""

import duckdb
import pydeequ
from pydeequ.v2.analyzers import (
    Size,
    Completeness,
    Distinctness,
    Mean,
    Minimum,
    Maximum,
    StandardDeviation,
    Correlation,
    Uniqueness,
)
from pydeequ.v2.checks import Check, CheckLevel
from pydeequ.v2.verification import AnalysisRunner, VerificationSuite
from pydeequ.v2.predicates import eq, gte, lte, between
from pydeequ.v2.profiles import ColumnProfilerRunner
from pydeequ.v2.suggestions import ConstraintSuggestionRunner, Rules


def create_sample_data(con: duckdb.DuckDBPyConnection):
    """Create a sample product reviews dataset for demonstration."""
    con.execute("""
        CREATE TABLE reviews AS SELECT * FROM (VALUES
            ('R001', 'C100', 'P001', 'US', 5, 10, 12, 2023, 'Great Product', 'Y'),
            ('R002', 'C101', 'P002', 'US', 4, 8, 10, 2023, 'Good Value', 'Y'),
            ('R003', 'C102', 'P001', 'UK', 5, 15, 18, 2022, 'Great Product', 'N'),
            ('R004', 'C103', 'P003', 'DE', 3, 5, 8, 2022, 'Decent Item', 'Y'),
            ('R005', 'C104', 'P002', 'FR', 4, 12, 15, 2021, 'Good Value', 'N'),
            ('R006', 'C105', 'P004', 'JP', 5, 20, 22, 2023, 'Excellent!', 'Y'),
            ('R007', 'C106', 'P001', 'US', 2, 3, 10, 2020, 'Great Product', 'N'),
            ('R008', 'C107', 'P005', 'UK', 1, 25, 30, 2021, 'Disappointing', 'Y'),
            ('R009', 'C108', 'P002', NULL, 4, 7, 9, 2023, 'Good Value', 'Y'),
            ('R001', 'C109', 'P003', 'US', 3, 4, 6, 2022, 'Decent Item', 'N')
        ) AS t(review_id, customer_id, product_id, marketplace, star_rating,
               helpful_votes, total_votes, review_year, product_title, insight)
    """)


def run_data_analysis(engine):
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

    result = (AnalysisRunner()
        .on_engine(engine)
        .addAnalyzer(Size())
        .addAnalyzer(Completeness("review_id"))
        .addAnalyzer(Completeness("marketplace"))
        .addAnalyzer(Distinctness(["review_id"]))
        .addAnalyzer(Mean("star_rating"))
        .addAnalyzer(Minimum("star_rating"))
        .addAnalyzer(Maximum("star_rating"))
        .addAnalyzer(StandardDeviation("star_rating"))
        .addAnalyzer(Correlation("total_votes", "helpful_votes"))
        .run())

    print("\nAnalysis Results:")
    print(result.to_string(index=False))

    # Extract key insights
    metrics = {(r["name"], r["instance"]): r["value"] for _, r in result.iterrows()}

    print("\nKey Insights:")
    print(f"  - Dataset contains {int(metrics.get(('Size', '*'), 0))} reviews")
    print(f"  - review_id completeness: {metrics.get(('Completeness', 'review_id'), 0):.1%}")
    print(f"  - marketplace completeness: {metrics.get(('Completeness', 'marketplace'), 0):.1%}")
    print(f"  - review_id distinctness: {metrics.get(('Distinctness', 'review_id'), 0):.1%}")
    print(f"  - Average star rating: {metrics.get(('Mean', 'star_rating'), 0):.2f}")
    print(f"  - Star rating range: {metrics.get(('Minimum', 'star_rating'), 0):.0f} - {metrics.get(('Maximum', 'star_rating'), 0):.0f}")

    return result


def run_constraint_verification(engine):
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

    result = (VerificationSuite()
        .on_engine(engine)
        .addCheck(check)
        .run())

    print("\nVerification Results:")
    print(result.to_string(index=False))

    # Summarize results
    passed = (result["constraint_status"] == "Success").sum()
    failed = (result["constraint_status"] == "Failure").sum()

    print(f"\nSummary: {passed} passed, {failed} failed out of {len(result)} constraints")

    if failed > 0:
        print("\nFailed Constraints:")
        for _, row in result[result["constraint_status"] == "Failure"].iterrows():
            print(f"  - {row['constraint']}")
            if row["constraint_message"]:
                print(f"    Message: {row['constraint_message']}")

    return result


def run_column_profiling(engine):
    """
    Run column profiling using ColumnProfilerRunner.

    This automatically computes statistics for each column:
    - Completeness
    - Approximate distinct values
    - Data type detection
    - Numeric statistics (mean, min, max, etc.)
    """
    print("\n" + "=" * 60)
    print("COLUMN PROFILING")
    print("=" * 60)

    result = (ColumnProfilerRunner()
        .on_engine(engine)
        .withLowCardinalityHistogramThreshold(10)  # Generate histograms for low-cardinality columns
        .run())

    print("\nColumn Profiles:")
    # Show selected columns for readability
    cols_to_show = ["column", "completeness", "approx_distinct_values", "data_type", "mean", "minimum", "maximum"]
    available_cols = [c for c in cols_to_show if c in result.columns]
    print(result[available_cols].to_string(index=False))

    return result


def run_constraint_suggestions(engine):
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

    result = (ConstraintSuggestionRunner()
        .on_engine(engine)
        .addConstraintRules(Rules.DEFAULT)
        .run())

    print("\nSuggested Constraints:")
    cols_to_show = ["column_name", "constraint_name", "description", "code_for_constraint"]
    available_cols = [c for c in cols_to_show if c in result.columns]
    print(result[available_cols].to_string(index=False))

    print(f"\nTotal suggestions: {len(result)}")

    return result


def main():
    print("PyDeequ Data Quality Example with DuckDB")
    print("No Spark cluster required!")

    # Create in-memory DuckDB connection
    con = duckdb.connect()

    # Create sample data
    print("\nCreating sample product reviews dataset...")
    create_sample_data(con)

    # Create engine using pydeequ.connect()
    engine = pydeequ.connect(con, table="reviews")

    print("\nDataset Schema:")
    schema = engine.get_schema()
    for col, dtype in schema.items():
        print(f"  {col}: {dtype}")

    print("\nSample Data:")
    print(con.execute("SELECT * FROM reviews LIMIT 5").fetchdf().to_string(index=False))

    # Run all examples
    run_data_analysis(engine)
    run_constraint_verification(engine)
    run_column_profiling(engine)
    run_constraint_suggestions(engine)

    print("\n" + "=" * 60)
    print("EXAMPLE COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
