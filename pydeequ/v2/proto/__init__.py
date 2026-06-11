# -*- coding: utf-8 -*-
"""
Generated protobuf classes for Deequ Spark Connect.

The canonical .proto schema lives in the Deequ repo. Per ADR-0006 (Stage 2),
the schema is split per surface: common.proto, verification.proto,
analysis.proto, column_profiler.proto, constraint_suggestion.proto. The
generated `*_pb2.py` and `*_pb2.pyi` files for each surface are checked in
alongside this package (GraphFrames pattern, ADR-0005). When the schema
changes, run `DEEQU_PROTO_DIR=... python scripts/regen_proto.py` to
refresh them.

A backwards-compatible facade module `deequ_connect_pb2` re-exports every
type so downstream pydeequ.v2 code can keep using the historical
`from pydeequ.v2.proto import deequ_connect_pb2 as proto` import idiom.
New code should prefer per-surface imports.
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
