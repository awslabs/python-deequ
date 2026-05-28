# -*- coding: utf-8 -*-
"""
Unit tests for SQL operators.

These tests verify the operator abstractions work correctly in isolation,
testing SQL generation and result extraction separately from actual
database execution.
"""

import pandas as pd
import pytest

from pydeequ.engines import MetricResult
from pydeequ.engines.operators import (
    # Scan operators
    SizeOperator,
    CompletenessOperator,
    MeanOperator,
    SumOperator,
    MinimumOperator,
    MaximumOperator,
    StandardDeviationOperator,
    MaxLengthOperator,
    MinLengthOperator,
    PatternMatchOperator,
    ComplianceOperator,
    CorrelationOperator,
    CountDistinctOperator,
    ApproxCountDistinctOperator,
    # Grouping operators
    DistinctnessOperator,
    UniquenessOperator,
    UniqueValueRatioOperator,
    EntropyOperator,
    MutualInformationOperator,
    # Factory
    OperatorFactory,
    # Mixins
    WhereClauseMixin,
    SafeExtractMixin,
    ColumnAliasMixin,
)


class TestWhereClauseMixin:
    """Tests for WhereClauseMixin."""

    def test_wrap_agg_with_where_no_condition(self):
        """Test wrapping aggregation without WHERE clause."""
        class TestClass(WhereClauseMixin):
            where = None

        obj = TestClass()
        result = obj.wrap_agg_with_where("AVG", "price")
        assert result == "AVG(price)"

    def test_wrap_agg_with_where_with_condition(self):
        """Test wrapping aggregation with WHERE clause."""
        class TestClass(WhereClauseMixin):
            where = "status = 'active'"

        obj = TestClass()
        result = obj.wrap_agg_with_where("AVG", "price")
        assert result == "AVG(CASE WHEN status = 'active' THEN price ELSE NULL END)"

    def test_wrap_count_with_where_no_condition(self):
        """Test wrapping COUNT without WHERE clause."""
        class TestClass(WhereClauseMixin):
            where = None

        obj = TestClass()
        result = obj.wrap_count_with_where()
        assert result == "COUNT(*)"

    def test_wrap_count_with_where_with_condition(self):
        """Test wrapping COUNT with WHERE clause."""
        class TestClass(WhereClauseMixin):
            where = "status = 'active'"

        obj = TestClass()
        result = obj.wrap_count_with_where()
        assert result == "SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END)"

    def test_wrap_count_with_where_custom_condition(self):
        """Test wrapping COUNT with custom condition and WHERE clause."""
        class TestClass(WhereClauseMixin):
            where = "status = 'active'"

        obj = TestClass()
        result = obj.wrap_count_with_where("price > 0")
        assert "status = 'active'" in result
        assert "price > 0" in result


class TestSafeExtractMixin:
    """Tests for SafeExtractMixin."""

    def test_safe_float_valid(self):
        """Test extracting valid float value."""
        class TestClass(SafeExtractMixin):
            pass

        obj = TestClass()
        df = pd.DataFrame({"value": [42.5]})
        result = obj.safe_float(df, "value")
        assert result == 42.5

    def test_safe_float_none(self):
        """Test extracting None value."""
        class TestClass(SafeExtractMixin):
            pass

        obj = TestClass()
        df = pd.DataFrame({"value": [None]})
        result = obj.safe_float(df, "value")
        assert result is None

    def test_safe_float_missing_column(self):
        """Test extracting from missing column."""
        class TestClass(SafeExtractMixin):
            pass

        obj = TestClass()
        df = pd.DataFrame({"other": [42.5]})
        result = obj.safe_float(df, "value")
        assert result is None

    def test_safe_int(self):
        """Test extracting integer value."""
        class TestClass(SafeExtractMixin):
            pass

        obj = TestClass()
        df = pd.DataFrame({"value": [42.7]})
        result = obj.safe_int(df, "value")
        assert result == 42


class TestColumnAliasMixin:
    """Tests for ColumnAliasMixin."""

    def test_make_alias_single_part(self):
        """Test alias with single part."""
        class TestClass(ColumnAliasMixin):
            pass

        obj = TestClass()
        result = obj.make_alias("mean", "price")
        assert result == "mean_price"

    def test_make_alias_multiple_parts(self):
        """Test alias with multiple parts."""
        class TestClass(ColumnAliasMixin):
            pass

        obj = TestClass()
        result = obj.make_alias("corr", "price", "quantity")
        assert result == "corr_price_quantity"

    def test_make_alias_sanitization(self):
        """Test alias sanitizes special characters."""
        class TestClass(ColumnAliasMixin):
            pass

        obj = TestClass()
        result = obj.make_alias("mean", "table.column")
        assert result == "mean_table_column"


class TestSizeOperator:
    """Tests for SizeOperator."""

    def test_get_aggregations_no_where(self):
        """Test SQL generation without WHERE clause."""
        op = SizeOperator()
        aggs = op.get_aggregations()
        assert len(aggs) == 1
        assert "COUNT(*)" in aggs[0]
        assert "size_value" in aggs[0]

    def test_get_aggregations_with_where(self):
        """Test SQL generation with WHERE clause."""
        op = SizeOperator(where="status = 'active'")
        aggs = op.get_aggregations()
        assert len(aggs) == 1
        assert "SUM(CASE WHEN" in aggs[0]
        assert "status = 'active'" in aggs[0]

    def test_extract_result(self):
        """Test result extraction."""
        op = SizeOperator()
        df = pd.DataFrame({"size_value": [100]})
        result = op.extract_result(df)
        assert result.name == "Size"
        assert result.instance == "*"
        assert result.entity == "Dataset"
        assert result.value == 100.0


class TestCompletenessOperator:
    """Tests for CompletenessOperator."""

    def test_get_aggregations(self):
        """Test SQL generation."""
        op = CompletenessOperator("email")
        aggs = op.get_aggregations()
        assert len(aggs) == 2
        assert any("count_email" in agg for agg in aggs)
        assert any("null_count_email" in agg for agg in aggs)

    def test_extract_result_complete(self):
        """Test result extraction with complete data."""
        op = CompletenessOperator("email")
        df = pd.DataFrame({
            "count_email": [100],
            "null_count_email": [0],
        })
        result = op.extract_result(df)
        assert result.value == 1.0

    def test_extract_result_partial(self):
        """Test result extraction with partial data."""
        op = CompletenessOperator("email")
        df = pd.DataFrame({
            "count_email": [100],
            "null_count_email": [20],
        })
        result = op.extract_result(df)
        assert result.value == 0.8


class TestMeanOperator:
    """Tests for MeanOperator."""

    def test_get_aggregations(self):
        """Test SQL generation."""
        op = MeanOperator("price")
        aggs = op.get_aggregations()
        assert len(aggs) == 1
        assert "AVG(price)" in aggs[0]
        assert "mean_price" in aggs[0]

    def test_extract_result(self):
        """Test result extraction."""
        op = MeanOperator("price")
        df = pd.DataFrame({"mean_price": [42.5]})
        result = op.extract_result(df)
        assert result.name == "Mean"
        assert result.instance == "price"
        assert result.value == 42.5


class TestPatternMatchOperator:
    """Tests for PatternMatchOperator."""

    def test_get_aggregations(self):
        """Test SQL generation."""
        op = PatternMatchOperator("email", r"^.+@.+\..+$")
        aggs = op.get_aggregations()
        assert len(aggs) == 2
        assert any("count_email" in agg for agg in aggs)
        assert any("pattern_match_email" in agg for agg in aggs)
        assert any("REGEXP_MATCHES" in agg for agg in aggs)

    def test_extract_result(self):
        """Test result extraction."""
        op = PatternMatchOperator("email", r"^.+@.+\..+$")
        df = pd.DataFrame({
            "count_email": [100],
            "pattern_match_email": [95],
        })
        result = op.extract_result(df)
        assert result.name == "PatternMatch"
        assert result.value == 0.95


class TestDistinctnessOperator:
    """Tests for DistinctnessOperator."""

    def test_get_grouping_columns(self):
        """Test grouping columns."""
        op = DistinctnessOperator(["category"])
        assert op.get_grouping_columns() == ["category"]

    def test_build_query(self):
        """Test query building."""
        op = DistinctnessOperator(["category"])
        query = op.build_query("products")
        assert "SELECT category" in query
        assert "GROUP BY category" in query
        assert "distinct_count" in query
        assert "total_count" in query

    def test_extract_result(self):
        """Test result extraction."""
        op = DistinctnessOperator(["category"])
        df = pd.DataFrame({
            "distinct_count": [10],
            "total_count": [100],
        })
        result = op.extract_result(df)
        assert result.name == "Distinctness"
        assert result.value == 0.1


class TestUniquenessOperator:
    """Tests for UniquenessOperator."""

    def test_build_query(self):
        """Test query building."""
        op = UniquenessOperator(["id"])
        query = op.build_query("users")
        assert "GROUP BY id" in query
        assert "HAVING" not in query  # HAVING is used in the inner query
        assert "unique_count" in query
        assert "total_count" in query

    def test_extract_result(self):
        """Test result extraction."""
        op = UniquenessOperator(["id"])
        df = pd.DataFrame({
            "unique_count": [90],
            "total_count": [100],
        })
        result = op.extract_result(df)
        assert result.name == "Uniqueness"
        assert result.value == 0.9


class TestEntropyOperator:
    """Tests for EntropyOperator."""

    def test_build_query(self):
        """Test query building."""
        op = EntropyOperator("category")
        query = op.build_query("products")
        assert "GROUP BY category" in query
        assert "LN" in query
        assert "entropy" in query

    def test_extract_result(self):
        """Test result extraction."""
        op = EntropyOperator("category")
        df = pd.DataFrame({"entropy": [2.5]})
        result = op.extract_result(df)
        assert result.name == "Entropy"
        assert result.value == 2.5


class TestOperatorFactory:
    """Tests for OperatorFactory."""

    def test_is_scan_operator(self):
        """Test scan operator detection."""
        from pydeequ.v2.analyzers import Mean, Sum, Completeness

        assert OperatorFactory.is_scan_operator(Mean("price"))
        assert OperatorFactory.is_scan_operator(Sum("amount"))
        assert OperatorFactory.is_scan_operator(Completeness("email"))

    def test_is_grouping_operator(self):
        """Test grouping operator detection."""
        from pydeequ.v2.analyzers import Distinctness, Uniqueness, Entropy

        assert OperatorFactory.is_grouping_operator(Distinctness("category"))
        assert OperatorFactory.is_grouping_operator(Uniqueness("id"))
        assert OperatorFactory.is_grouping_operator(Entropy("status"))

    def test_create_scan_operator(self):
        """Test creating scan operator from analyzer."""
        from pydeequ.v2.analyzers import Mean

        analyzer = Mean("price", where="status = 'active'")
        operator = OperatorFactory.create(analyzer)

        assert operator is not None
        assert isinstance(operator, MeanOperator)
        assert operator.column == "price"
        assert operator.where == "status = 'active'"

    def test_create_grouping_operator(self):
        """Test creating grouping operator from analyzer."""
        from pydeequ.v2.analyzers import Distinctness

        analyzer = Distinctness(["category", "brand"])
        operator = OperatorFactory.create(analyzer)

        assert operator is not None
        assert isinstance(operator, DistinctnessOperator)
        assert operator.columns == ["category", "brand"]

    def test_is_supported(self):
        """Test analyzer support checking."""
        from pydeequ.v2.analyzers import Mean, Histogram, ApproxQuantile, DataType

        assert OperatorFactory.is_supported(Mean("price"))
        # Histogram and ApproxQuantile are now supported as operators
        assert OperatorFactory.is_supported(Histogram("category"))
        assert OperatorFactory.is_supported(ApproxQuantile("price", 0.5))
        # DataType is now supported via the metadata registry
        assert OperatorFactory.is_supported(DataType("category"))
        assert OperatorFactory.is_metadata_operator(DataType("category"))


class TestOperatorIntegration:
    """Integration tests for operators with actual DuckDB."""

    @pytest.fixture
    def duckdb_conn(self):
        """Create a DuckDB connection with test data."""
        import duckdb

        conn = duckdb.connect(":memory:")
        conn.execute("""
            CREATE TABLE test_data AS SELECT * FROM (
                VALUES
                    (1, 'Alice', 100.0, 'A', 'active'),
                    (2, 'Bob', 200.0, 'B', 'active'),
                    (3, 'Carol', 150.0, 'A', 'inactive'),
                    (4, 'Dave', NULL, 'C', 'active'),
                    (5, 'Eve', 300.0, 'A', 'active')
            ) AS t(id, name, amount, category, status)
        """)
        yield conn
        conn.close()

    def test_scan_operators_batch_execution(self, duckdb_conn):
        """Test batch execution of multiple scan operators."""
        operators = [
            SizeOperator(),
            MeanOperator("amount"),
            MaximumOperator("amount"),
            MinimumOperator("amount"),
        ]

        # Collect all aggregations
        aggregations = []
        for op in operators:
            aggregations.extend(op.get_aggregations())

        # Execute single query
        query = f"SELECT {', '.join(aggregations)} FROM test_data"
        result = duckdb_conn.execute(query).fetchdf()

        # Extract results
        results = [op.extract_result(result) for op in operators]

        assert results[0].value == 5.0  # Size
        assert results[1].value == 187.5  # Mean (750/4 non-null)
        assert results[2].value == 300.0  # Maximum
        assert results[3].value == 100.0  # Minimum

    def test_grouping_operator_execution(self, duckdb_conn):
        """Test execution of grouping operator."""
        op = DistinctnessOperator(["category"])
        query = op.build_query("test_data")
        result = duckdb_conn.execute(query).fetchdf()
        metric = op.extract_result(result)

        # 3 distinct categories / 5 rows = 0.6
        assert metric.name == "Distinctness"
        assert metric.value == 0.6

    def test_completeness_operator(self, duckdb_conn):
        """Test completeness operator with NULL values."""
        op = CompletenessOperator("amount")
        aggs = op.get_aggregations()
        query = f"SELECT {', '.join(aggs)} FROM test_data"
        result = duckdb_conn.execute(query).fetchdf()
        metric = op.extract_result(result)

        # 4 non-null out of 5
        assert metric.value == 0.8

    def test_operator_with_where_clause(self, duckdb_conn):
        """Test operator with WHERE clause filtering."""
        op = MeanOperator("amount", where="status = 'active'")
        aggs = op.get_aggregations()
        query = f"SELECT {', '.join(aggs)} FROM test_data"
        result = duckdb_conn.execute(query).fetchdf()
        metric = op.extract_result(result)

        # Active rows: 100, 200, NULL, 300 -> mean of non-null = 200
        assert metric.value == 200.0
