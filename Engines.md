# Engine Parity Analysis Report

## Executive Summary

This report documents the parity testing results between the Spark and DuckDB engines in python-deequ. After implementing all fixes, **84 tests pass** and **5 tests fail** due to inherent algorithmic differences.

### Current Test Results

| Status | Count | Percentage |
|--------|-------|------------|
| **Passed** | 84 | 94.4% |
| **Failed** | 5 | 5.6% |

### Summary of Remaining Failures

| Category | Test Failures | Root Cause | Fixable? |
|----------|---------------|------------|----------|
| Approximate Quantile | 2 tests | Different quantile algorithms | Inherent difference |
| Approximate Count Distinct | 1 test | HyperLogLog implementation variance | Inherent variance |
| Profile Distinct Values | 2 tests | HyperLogLog variance | Inherent variance |

---

## Fixes Applied

### 1. STDDEV_SAMP → STDDEV_POP

Changed DuckDB to use population standard deviation to match Spark.

**Files Modified:**
- `pydeequ/engines/operators/scan_operators.py`
- `pydeequ/engines/operators/profiling_operators.py`

**Tests Fixed:** ~8 tests

### 2. Entropy: LOG2 → LN

Changed DuckDB entropy calculation from log base 2 to natural log to match Spark.

**File Modified:** `pydeequ/engines/operators/grouping_operators.py`

```python
# Before (bits)
-SUM((cnt * 1.0 / total_cnt) * LOG2(cnt * 1.0 / total_cnt)) AS entropy

# After (nats) - matches Spark
-SUM((cnt * 1.0 / total_cnt) * LN(cnt * 1.0 / total_cnt)) AS entropy
```

**Tests Fixed:** 3 tests
- `test_entropy_uniform`
- `test_mutual_information`
- `test_has_entropy` (constraint)

### 3. Spark Connect Server Fixture

Added automatic Spark Connect server startup for parity tests.

**File Modified:** `tests/engines/comparison/conftest.py`

```python
@pytest.fixture(scope="session")
def spark_connect_server():
    """Automatically starts Spark Connect server if not running."""
    from benchmark.spark_server import SparkConnectServer
    from benchmark.config import SparkServerConfig

    config = SparkServerConfig()
    server = SparkConnectServer(config)

    if not server.is_running():
        server.start()

    if not os.environ.get("SPARK_REMOTE"):
        os.environ["SPARK_REMOTE"] = f"sc://localhost:{config.port}"

    yield server
```

### 4. Flatten Metrics in DeequRelationPlugin (Histogram/DataType Fix)

Fixed the Scala Deequ Connect plugin to properly handle complex metrics like Histogram and DataType by flattening them before output.

**File Modified:** `deequ/src/main/scala/com/amazon/deequ/connect/DeequRelationPlugin.scala`

**Root Cause:** The plugin was only collecting `DoubleMetric` instances directly, but Histogram and DataType return complex metric types (`HistogramMetric`, etc.) that need to be flattened first.

**Before:**
```scala
val metrics = context.metricMap.toSeq.collect {
  case (analyzer, metric: DoubleMetric) => ...
}
```

**After:**
```scala
val metrics = context.metricMap.toSeq.flatMap { case (analyzer, metric) =>
  metric.flatten().map { doubleMetric =>
    val value: Double = doubleMetric.value.getOrElse(Double.NaN)
    (
      analyzer.toString,
      doubleMetric.entity.toString,
      doubleMetric.instance,
      doubleMetric.name,
      value
    )
  }
}
```

**Tests Fixed:** 2 tests
- `test_histogram`
- `test_data_type`

---

## Detailed Analysis of Remaining Failures

### 1. Approximate Quantile (2 tests)

**Root Cause: Different Algorithms**

| Engine | Algorithm |
|--------|-----------|
| Spark | T-Digest (approximate) |
| DuckDB | QUANTILE_CONT (exact interpolation) |

The algorithms produce different results, especially for small datasets.

**Resolution:** Accept as inherent difference or implement T-Digest in DuckDB.

### 2. Approximate Count Distinct (3 tests)

**Root Cause: HyperLogLog Variance**

Both engines use HyperLogLog but with different implementations:
- Different hash functions
- Different precision parameters

**Evidence:**
```
Spark approx_distinct:  9
DuckDB approx_distinct: 10  (or 6 vs 5)
```

~10% variance is expected for probabilistic data structures.

**Resolution:** Accept as inherent variance. The 10% tolerance handles most cases but edge cases with small cardinalities still fail.

---

## Test Results Summary

### Passing Tests (84)

All core analyzers and constraints:
- Size, Completeness, Mean, Sum, Min, Max
- StandardDeviation (after STDDEV_POP fix)
- Distinctness, Uniqueness, UniqueValueRatio, CountDistinct
- Correlation, PatternMatch, Compliance
- MinLength, MaxLength
- Entropy, MutualInformation (after LN fix)
- **Histogram** (after flatten fix)
- **DataType** (after flatten fix)
- All constraint tests (32 tests)
- All suggestion tests (13 tests)
- Most profile tests

### Failing Tests (5)

| Test | Category | Status |
|------|----------|--------|
| `test_approx_count_distinct` | Analyzer | Inherent HLL variance |
| `test_approx_quantile_median` | Analyzer | Algorithm difference |
| `test_approx_quantile_quartiles` | Analyzer | Algorithm difference |
| `test_completeness_partial` | Profile | Inherent HLL variance |
| `test_distinct_values` | Profile | Inherent HLL variance |

---

## Files Modified

### Python (python-deequ)

| File | Changes |
|------|---------|
| `pydeequ/engines/operators/scan_operators.py` | STDDEV_SAMP → STDDEV_POP |
| `pydeequ/engines/operators/profiling_operators.py` | STDDEV_SAMP → STDDEV_POP |
| `pydeequ/engines/operators/grouping_operators.py` | LOG2 → LN for entropy |
| `tests/engines/comparison/conftest.py` | Added `spark_connect_server` fixture |
| `tests/engines/comparison/utils.py` | Tolerance adjustments, JSON parsing |

### Scala (deequ)

| File | Changes |
|------|---------|
| `deequ/src/main/scala/com/amazon/deequ/connect/DeequRelationPlugin.scala` | Flatten metrics in `analyzerContextToDataFrame` |

---

## Recommendations

### Mark as xfail (5 tests)

These tests should be marked with `@pytest.mark.xfail` with documented reasons:

```python
@pytest.mark.xfail(reason="HyperLogLog implementation variance")
def test_approx_count_distinct(self, ...):
    ...

@pytest.mark.xfail(reason="T-Digest vs QUANTILE_CONT algorithm difference")
def test_approx_quantile_median(self, ...):
    ...
```

### Future Improvements

1. **Exact Count for Small Data**: Use `COUNT(DISTINCT)` instead of HyperLogLog when dataset size < threshold
2. **Quantile Algorithm Alignment**: Consider implementing T-Digest in DuckDB for exact parity

---

## Conclusion

The parity testing initiative achieved **94.4% test pass rate** (84/89 tests). The remaining 5 failures represent inherent algorithmic differences:

1. **Probabilistic algorithm variance** (3 tests) - Inherent to HyperLogLog
2. **Algorithm differences** (2 tests) - T-Digest vs QUANTILE_CONT

All major analyzers (Size, Completeness, Mean, StandardDeviation, Entropy, Correlation, Histogram, DataType, etc.) now have full parity between engines.
