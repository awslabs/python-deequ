# -*- coding: utf-8 -*-
"""
Metadata operator implementations.

Metadata operators compute metrics using schema information rather than
SQL aggregations. They are useful for type inference and schema-based
analysis that don't require scanning data.
"""

from __future__ import annotations

import json
from typing import Dict, Optional

from pydeequ.engines import MetricResult
from pydeequ.engines.operators.mixins import (
    ColumnAliasMixin,
    SafeExtractMixin,
)

class DataTypeOperator(SafeExtractMixin, ColumnAliasMixin):
    """
    Computes data type information from schema metadata.

    Unlike scan operators that require SQL queries, DataTypeOperator
    infers type information directly from the table schema, making it
    more efficient for type analysis.

    Type Mapping:
        DuckDB types are mapped to Deequ-compatible type categories:
        - Integral: TINYINT, SMALLINT, INTEGER, BIGINT, HUGEINT, etc.
        - Fractional: FLOAT, DOUBLE, REAL, DECIMAL, NUMERIC
        - String: VARCHAR, CHAR, TEXT, etc.
        - Boolean: BOOLEAN, BOOL

    Attributes:
        column: Column name to analyze
        where: Optional WHERE clause (ignored for schema-based inference)
    """

    # Mapping from DuckDB SQL types to Deequ type categories
    TYPE_MAPPING: Dict[str, str] = {
        # Integral types
        "TINYINT": "Integral",
        "SMALLINT": "Integral",
        "INTEGER": "Integral",
        "BIGINT": "Integral",
        "HUGEINT": "Integral",
        "UTINYINT": "Integral",
        "USMALLINT": "Integral",
        "UINTEGER": "Integral",
        "UBIGINT": "Integral",
        "INT": "Integral",
        "INT1": "Integral",
        "INT2": "Integral",
        "INT4": "Integral",
        "INT8": "Integral",
        # Fractional types
        "FLOAT": "Fractional",
        "DOUBLE": "Fractional",
        "REAL": "Fractional",
        "DECIMAL": "Fractional",
        "NUMERIC": "Fractional",
        "FLOAT4": "Fractional",
        "FLOAT8": "Fractional",
        # String types
        "VARCHAR": "String",
        "CHAR": "String",
        "BPCHAR": "String",
        "TEXT": "String",
        "STRING": "String",
        # Boolean types
        "BOOLEAN": "Boolean",
        "BOOL": "Boolean",
        # Date/Time types (mapped to String for Deequ compatibility)
        "DATE": "String",
        "TIMESTAMP": "String",
        "TIME": "String",
        "TIMESTAMPTZ": "String",
        "TIMETZ": "String",
        "INTERVAL": "String",
        # Binary types
        "BLOB": "Unknown",
        "BYTEA": "Unknown",
        # UUID
        "UUID": "String",
    }

    def __init__(self, column: str, where: Optional[str] = None):
        """
        Initialize DataTypeOperator.

        Args:
            column: Column name to analyze
            where: Optional WHERE clause (ignored for schema-based inference)
        """
        self.column = column
        self.where = where  # Stored but ignored for schema-based type inference

    @property
    def metric_name(self) -> str:
        """Return the metric name for this operator."""
        return "DataType"

    @property
    def instance(self) -> str:
        """Return the instance identifier for this operator."""
        return self.column

    @property
    def entity(self) -> str:
        """Return the entity type for this operator."""
        return "Column"

    def compute_from_schema(self, schema: Dict[str, str]) -> MetricResult:
        """
        Compute data type information from schema.

        Args:
            schema: Dictionary mapping column names to SQL type names

        Returns:
            MetricResult with JSON-encoded type information
        """
        sql_type = schema.get(self.column, "Unknown")
        mapped_type = self.TYPE_MAPPING.get(sql_type, "Unknown")

        # Build result compatible with Spark Deequ format
        result = {
            "dtype": sql_type,
            "mapped_type": mapped_type,
            "type_counts": {mapped_type: 1.0}  # DuckDB has strict typing
        }

        return MetricResult(
            name=self.metric_name,
            instance=self.instance,
            entity=self.entity,
            value=json.dumps(result),
        )


__all__ = [
    "DataTypeOperator",
]
