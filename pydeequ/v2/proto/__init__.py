# -*- coding: utf-8 -*-
"""
Generated protobuf classes for Deequ Spark Connect.

The canonical .proto schema lives in the Deequ repo. The generated
_pb2.py and _pb2.pyi files in this package are checked in and committed
alongside the source (matching the GraphFrames pattern). When the schema
changes, run `python scripts/regen_proto.py DEEQU_JAR_PATH=...` to refresh
them; CI verifies the checked-in stubs match the pinned JAR.

See ADR-0005 in the deequ repo.
"""

from pydeequ.v2.proto.deequ_connect_pb2 import (
    Analyzer,
    Check,
    CheckLevel,
    Constraint,
    ConstraintRuleSet,
    DeequAnalysisRelation,
    DeequColumnProfilerRelation,
    DeequConstraintSuggestionRelation,
    DeequVerificationRelation,
    KLLParameters,
    Predicate,
    # Spec submessages (see CONTEXT.md in the deequ repo)
    AllowedValuesSpec,
    ApproxQuantileSpec,
    ApproxQuantilesSpec,
    ColumnAnalyzerSpec,
    ColumnAssertionSpec,
    ColumnSpec,
    ColumnsAnalyzerSpec,
    ColumnsAssertionSpec,
    ColumnsSpec,
    ComplianceAnalyzerSpec,
    EmptySpec,
    HistogramSpec,
    PairColumnsAnalyzerSpec,
    PairColumnsAssertionSpec,
    PatternAssertionSpec,
    PatternMatchSpec,
    PrimaryKeySpec,
    QuantileAssertionSpec,
    SatisfiesSpec,
    SizeSpec,
)

__all__ = [
    "Analyzer",
    "Check",
    "CheckLevel",
    "Constraint",
    "ConstraintRuleSet",
    "DeequAnalysisRelation",
    "DeequColumnProfilerRelation",
    "DeequConstraintSuggestionRelation",
    "DeequVerificationRelation",
    "KLLParameters",
    "Predicate",
    "AllowedValuesSpec",
    "ApproxQuantileSpec",
    "ApproxQuantilesSpec",
    "ColumnAnalyzerSpec",
    "ColumnAssertionSpec",
    "ColumnSpec",
    "ColumnsAnalyzerSpec",
    "ColumnsAssertionSpec",
    "ColumnsSpec",
    "ComplianceAnalyzerSpec",
    "EmptySpec",
    "HistogramSpec",
    "PairColumnsAnalyzerSpec",
    "PairColumnsAssertionSpec",
    "PatternAssertionSpec",
    "PatternMatchSpec",
    "PrimaryKeySpec",
    "QuantileAssertionSpec",
    "SatisfiesSpec",
    "SizeSpec",
]
