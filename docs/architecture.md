# PyDeequ v2 Architecture

## Overview

PyDeequ v2 introduces a multi-engine architecture enabling data quality checks on different backends. The code is the source of truth - this document provides a high-level map to help you navigate the codebase.

**Supported backends:**
- **DuckDB**: Local development, small-medium datasets (`pip install duckdb`)
- **Spark Connect**: Large-scale distributed processing (requires Spark cluster)

## Design Philosophy

The architecture is inspired by [DuckDQ](https://github.com/tdoehmen/duckdq), which demonstrated a key insight:

> **Decouple state computation (engine-dependent) from state merging (engine-independent)**

- **State computation** = expensive, engine-dependent (SQL queries, Spark jobs)
- **State merging** = cheap, pure Python (addition, max/min, Welford's algorithm)

This separation enables multiple backends, incremental validation, and distributed processing.

## Architecture Diagram

```
                    ┌──────────────────────────────────────┐
                    │            User API                  │
                    │  VerificationSuite, AnalysisRunner   │
                    │  ColumnProfilerRunner, Suggestions   │
                    └─────────────────┬────────────────────┘
                                      │
                    ┌─────────────────▼────────────────────┐
                    │         Engine Abstraction           │
                    │           BaseEngine ABC             │
                    │  compute_metrics(), run_checks()     │
                    │  profile_columns(), suggest_...()    │
                    └─────────────────┬────────────────────┘
                                      │
              ┌───────────────────────┼───────────────────────┐
              │                       │                       │
    ┌─────────▼─────────┐   ┌─────────▼─────────┐   ┌─────────▼─────────┐
    │   DuckDBEngine    │   │   SparkEngine     │   │  Future Engines   │
    │   (Direct SQL)    │   │ (Spark Connect)   │   │ (Polars, etc.)    │
    └───────────────────┘   └───────────────────┘   └───────────────────┘
```

## Module Structure

```
pydeequ/
├── __init__.py                    # connect() with auto-detection
├── engines/
│   ├── __init__.py                # BaseEngine ABC, result types
│   ├── duckdb.py                  # DuckDBEngine implementation
│   ├── spark.py                   # SparkEngine wrapper
│   ├── operators/
│   │   ├── base.py                # ScanOperator, GroupingOperator ABCs
│   │   ├── factory.py             # OperatorFactory registry
│   │   ├── mixins.py              # WhereClauseMixin, SafeExtractMixin
│   │   ├── scan_operators.py      # 15 single-pass operators
│   │   ├── grouping_operators.py  # 6 GROUP BY operators
│   │   ├── metadata_operators.py  # Schema-based operators
│   │   └── profiling_operators.py # Column profiling operators
│   ├── constraints/
│   │   ├── base.py                # BaseEvaluator hierarchy
│   │   ├── factory.py             # ConstraintEvaluatorFactory (27 types)
│   │   └── evaluators.py          # 23 concrete evaluators
│   └── suggestions/
│       ├── runner.py              # Suggestion generation
│       ├── rules.py               # Rule implementations
│       └── registry.py            # Rule registry
└── v2/                            # User-facing API
    ├── analyzers.py               # Analyzer definitions
    ├── checks.py                  # Check/Constraint definitions
    ├── predicates.py              # Predicate classes
    ├── verification.py            # VerificationSuite, AnalysisRunner
    ├── profiles.py                # ColumnProfilerRunner
    └── suggestions.py             # ConstraintSuggestionRunner
```

## Key Abstractions

### BaseEngine (`pydeequ/engines/__init__.py`)

Abstract base class defining the engine interface. All engines implement:
- `compute_metrics(analyzers)` - Run analyzers and return `MetricResult` list
- `run_checks(checks)` - Evaluate constraints and return `ConstraintResult` list
- `profile_columns(columns)` - Return `ColumnProfile` for each column
- `suggest_constraints(rules)` - Generate `ConstraintSuggestion` list

### Operators (`pydeequ/engines/operators/`)

Operators translate analyzers into engine-specific queries:

| Type | Description | Examples |
|------|-------------|----------|
| **ScanOperator** | Single-pass SQL aggregations, batched together | Size, Completeness, Mean, Sum, Min, Max |
| **GroupingOperator** | Requires GROUP BY, runs individually | Distinctness, Uniqueness, Entropy |
| **MetadataOperator** | Schema-based, no query needed | DataType |

See `base.py` for ABCs, `factory.py` for the registry pattern.

### OperatorFactory (`pydeequ/engines/operators/factory.py`)

Registry mapping analyzer names to operator classes. Use `OperatorFactory.create(analyzer)` to instantiate operators. The factory determines query batching strategy.

### Constraint Evaluators (`pydeequ/engines/constraints/`)

Evaluators check if computed metrics satisfy constraints:
- **AnalyzerBasedEvaluator**: Delegates to an analyzer operator (hasMean, hasMin)
- **RatioCheckEvaluator**: Computes matches/total ratio (isPositive, isContainedIn)

The `ConstraintEvaluatorFactory` maps 27 constraint types to evaluator classes.

### Result Types (`pydeequ/engines/__init__.py`)

Standardized dataclasses returned by all engines:
- `MetricResult`: Analyzer output (name, column, value, success)
- `ConstraintResult`: Check output (constraint, status, message)
- `ColumnProfile`: Profiling output (column, stats, histogram)

All convert to pandas DataFrames via `results_to_dataframe()`.

## Quick Start Examples

### Analysis

```python
import duckdb
import pydeequ
from pydeequ.v2.analyzers import Size, Completeness, Mean
from pydeequ.v2.verification import AnalysisRunner

con = duckdb.connect()
con.execute("CREATE TABLE sales AS SELECT * FROM 'sales.parquet'")
engine = pydeequ.connect(con, table="sales")

result = (AnalysisRunner()
    .on_engine(engine)
    .addAnalyzer(Size())
    .addAnalyzer(Completeness("customer_id"))
    .addAnalyzer(Mean("amount"))
    .run())
```

### Verification

```python
from pydeequ.v2.checks import Check, CheckLevel
from pydeequ.v2.verification import VerificationSuite
from pydeequ.v2.predicates import gte

result = (VerificationSuite()
    .on_engine(engine)
    .addCheck(
        Check(CheckLevel.Error, "Data Quality")
            .isComplete("id")
            .hasCompleteness("email", gte(0.95))
            .isUnique("transaction_id")
    )
    .run())
```

### Profiling

```python
from pydeequ.v2.profiles import ColumnProfilerRunner

profiles = (ColumnProfilerRunner()
    .on_engine(engine)
    .run())
```

### Suggestions

```python
from pydeequ.v2.suggestions import ConstraintSuggestionRunner, Rules

suggestions = (ConstraintSuggestionRunner()
    .on_engine(engine)
    .addConstraintRules(Rules.DEFAULT)
    .run())
```

## Engine Comparison

| Aspect | DuckDB | Spark |
|--------|--------|-------|
| **Use case** | Local dev, CI/CD, files < 10GB | Distributed data, data lakes |
| **Setup** | `pip install duckdb` | Spark cluster + Deequ plugin |
| **Latency** | Low (in-process) | Higher (network overhead) |
| **Scaling** | Single-node, memory-bound | Distributed, scales horizontally |
| **Approximate metrics** | HyperLogLog, exact quantiles | HLL++, KLL sketches |

Both engines aim for functional parity. Minor differences exist in approximate algorithms and histogram formats - see test suite for tolerances.

## Benchmarks

Performance comparisons between DuckDB and Spark engines are documented in [BENCHMARK.md](../BENCHMARK.md), including:
- Varying row counts (100K to 130M rows)
- Varying column counts (10 to 80 columns)
- Column profiling performance

## Future Enhancements

- State persistence for incremental validation
- Additional backends (Polars, SQLAlchemy, BigQuery)
- Anomaly detection on metrics
- Data lineage for constraint violations

## References

- [DuckDQ](https://github.com/tdoehmen/duckdq) - Inspiration for engine abstraction
- [AWS Deequ](https://github.com/awslabs/deequ) - Original Scala implementation
- [Ibis](https://ibis-project.org/) - Multi-backend design patterns
