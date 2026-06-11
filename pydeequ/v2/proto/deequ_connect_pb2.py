# -*- coding: utf-8 -*-
"""
Legacy single-module facade over the per-surface generated stubs.

The schema was split per surface in Stage 2 (ADR-0006). Downstream pydeequ
modules import via `from pydeequ.v2.proto import deequ_connect_pb2 as proto`
and reference attributes like `proto.Constraint`, `proto.Predicate`, etc.
This module re-exports those names from the per-surface `_pb2.py` files so
the existing import idiom keeps working unchanged.

If you're writing new code, prefer importing per-surface
(e.g. `from pydeequ.v2.proto import verification_pb2`) — the facade exists
to keep the diff in Stage 2 small.
"""

# common.proto — Predicate, KLLParameters, CheckLevel, ConstraintRuleSet
from pydeequ.v2.proto.common_pb2 import (  # noqa: F401
    Predicate,
    KLLParameters,
    CheckLevel,
    ConstraintRuleSet,
)

# verification.proto — Constraint, Check, DeequVerificationRelation, plus
# the Constraint-side …Spec submessages
from pydeequ.v2.proto.verification_pb2 import (  # noqa: F401
    AllowedValuesSpec,
    Check,
    ColumnAssertionSpec,
    ColumnSpec,
    ColumnsAssertionSpec,
    ColumnsSpec,
    Constraint,
    DeequVerificationRelation,
    PairColumnsAssertionSpec,
    PatternAssertionSpec,
    PrimaryKeySpec,
    QuantileAssertionSpec,
    SatisfiesSpec,
    SizeSpec,
)

# analysis.proto — Analyzer, DeequAnalysisRelation, plus the Analyzer-side …Spec submessages
from pydeequ.v2.proto.analysis_pb2 import (  # noqa: F401
    Analyzer,
    ApproxQuantileSpec,
    ApproxQuantilesSpec,
    ColumnAnalyzerSpec,
    ColumnsAnalyzerSpec,
    ComplianceAnalyzerSpec,
    DeequAnalysisRelation,
    EmptySpec,
    HistogramSpec,
    PairColumnsAnalyzerSpec,
    PatternMatchSpec,
)

# column_profiler.proto
from pydeequ.v2.proto.column_profiler_pb2 import (  # noqa: F401
    DeequColumnProfilerRelation,
)

# constraint_suggestion.proto
from pydeequ.v2.proto.constraint_suggestion_pb2 import (  # noqa: F401
    DeequConstraintSuggestionRelation,
)
