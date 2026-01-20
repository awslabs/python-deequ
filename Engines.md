# Engine Parity Analysis Report

## Executive Summary

This report documents 18 remaining parity test failures between the Spark and DuckDB engines in python-deequ. The failures are grouped by root cause to facilitate systematic resolution.

### Summary by Category

| Category | Test Failures | Root Cause |
|----------|---------------|------------|
| Standard Deviation | 2 tests | Population vs Sample formula |
| Approximate Algorithms | 3 tests | Different HyperLogLog/quantile implementations |
| Information Theory | 3 tests | Floating-point precision in log calculations |
| Output Format | 2 tests | JSON string vs structured Map |
| Profile Statistics | 8 tests | Combination of above issues |

### Impact Assessment

- **Critical**: Standard deviation and output format differences affect basic analyzer functionality
- **Moderate**: Approximate algorithm differences are inherent to probabilistic data structures
- **Low**: Information theory precision differences are minor numerical variations

### Recommended Priority Order

1. Output format differences (quick fix, high impact)
2. Standard deviation formula alignment (configuration option)
3. Tolerance adjustments for approximate metrics
4. Information theory numerical stability (long-term)

---

## Detailed Analysis by Category

### 2.1 Standard Deviation Differences

**Affected Tests:**
- `test_standard_deviation`
- `test_numeric_statistics` (profile)
- `test_all_basic_analyzers`

**Root Cause:**

DuckDB uses `STDDEV_SAMP()` which implements the sample standard deviation formula with N-1 denominator (Bessel's correction). Spark may use the population formula (N denominator) or a different algorithm.

**Evidence:**

```
Spark:  std_dev(price) = 11.18 (population formula)
DuckDB: std_dev(price) = 12.91 (sample formula)
```

The relationship between these values follows the expected formula:
- Sample std = Population std × √(N/(N-1))
- For small datasets, this difference is significant

**Options to Bridge:**

| Option | Description | Pros | Cons |
|--------|-------------|------|------|
| 1. Config option | Add setting to switch between `STDDEV_SAMP`/`STDDEV_POP` | Flexible, user choice | More complexity |
| 2. Match Spark | Determine which formula Spark uses and replicate | True parity | May not match DuckDB conventions |
| 3. Increase tolerance | Relax test assertions for statistical metrics | Quick fix | Doesn't address root cause |

**Recommended Fix:** Option 1 - Add configuration option with Spark's formula as default for parity mode.

---

### 2.2 Approximate Count Distinct

**Affected Tests:**
- `test_approx_count_distinct`
- `test_distinct_values` (profile)

**Root Cause:**

Different HyperLogLog implementations with different precision parameters:
- Spark uses HyperLogLog++ with configurable precision (default p=14)
- DuckDB uses its own HyperLogLog implementation

**Evidence:**

Approximate distinct counts can vary by several percent between engines due to:
- Different hash functions
- Different precision parameters
- Different merging algorithms

**Options to Bridge:**

| Option | Description | Pros | Cons |
|--------|-------------|------|------|
| 1. Exact count for small data | Use `COUNT(DISTINCT)` when dataset is small | Accurate for tests | Performance impact at scale |
| 2. Increase tolerance | Allow >10% variance for approximate metrics | Realistic expectation | Less precise tests |
| 3. Document as expected | Mark as known variance in documentation | Honest about limitations | Doesn't "fix" tests |

**Recommended Fix:** Option 1 for test datasets, Option 2 for production with appropriate tolerance.

---

### 2.3 Entropy & Mutual Information

**Affected Tests:**
- `test_entropy_uniform`
- `test_mutual_information`
- `test_has_entropy` (constraint)

**Root Cause:**

Floating-point precision differences in logarithmic calculations:
- Log base conversions (ln vs log2)
- Numerical stability in probability calculations
- Handling of edge cases (p=0, p=1)

**Evidence:**

```
Spark entropy:  2.3219280948873626
DuckDB entropy: 2.321928094887362 (may differ in last digits)
```

Differences in the 15th-16th decimal places are common due to:
- Different order of floating-point operations
- Different handling of -0 × log(0) = 0 convention

**Options to Bridge:**

| Option | Description | Pros | Cons |
|--------|-------------|------|------|
| 1. Round before compare | Round to fewer decimal places | Simple fix | Loses precision info |
| 2. Log-sum-exp trick | Use numerically stable algorithms | Mathematically correct | Implementation effort |
| 3. Increase tolerance | Use relative tolerance ~1e-10 | Acknowledges FP limits | May mask real bugs |

**Recommended Fix:** Option 3 - Use relative tolerance appropriate for floating-point comparison.

---

### 2.4 Quantile Calculations

**Affected Tests:**
- `test_approx_quantile_median`
- `test_approx_quantile_quartiles`

**Root Cause:**

Different interpolation methods for percentile calculation:
- Spark uses T-Digest algorithm for approximate quantiles
- DuckDB uses `QUANTILE_CONT` (continuous) or `QUANTILE_DISC` (discrete)

**Evidence:**

For a dataset [1, 2, 3, 4, 5]:
```
Median with interpolation: 3.0
Median discrete: 3
Q1 with interpolation: 1.75 or 2.0 (depends on method)
```

There are 9 different interpolation methods defined in statistics literature.

**Options to Bridge:**

| Option | Description | Pros | Cons |
|--------|-------------|------|------|
| 1. Use PERCENTILE_DISC | Exact percentiles, no interpolation | Deterministic | Different semantics |
| 2. Increase tolerance | Allow small variance in quantiles | Pragmatic | Imprecise tests |
| 3. Document methods | Specify interpolation method used | Clear expectations | Doesn't achieve parity |

**Recommended Fix:** Option 2 with documentation of method differences (Option 3).

---

### 2.5 Output Format Differences

**Affected Tests:**
- `test_histogram`
- `test_data_type`

**Root Cause:**

Different output formats for complex types:
- DuckDB returns JSON strings for maps/structs: `'{"bin1": 10, "bin2": 20}'`
- Spark returns native structured Maps: `Map(bin1 -> 10, bin2 -> 20)`

**Evidence:**

```python
# DuckDB histogram output
'{"[0,10)": 5, "[10,20)": 3, "[20,30)": 2}'

# Spark histogram output
{'[0,10)': 5, '[10,20)': 3, '[20,30)': 2}
```

**Options to Bridge:**

| Option | Description | Pros | Cons |
|--------|-------------|------|------|
| 1. Parse JSON in comparison | Convert JSON strings to dicts before compare | Quick fix | Only fixes tests |
| 2. Standardize output | Return same format from both engines | Consistent API | Breaking change |
| 3. Format-agnostic comparison | Utility that handles both formats | Flexible | More code |

**Recommended Fix:** Option 1 (immediate) + Option 2 (future standardization).

---

### 2.6 Profile Edge Cases

**Affected Tests:**
- `test_profile_all_columns`
- `test_profile_specific_columns`
- `test_completeness_partial`
- `test_numeric_with_nulls`
- `test_single_row`
- `test_mixed_types`

**Root Causes:**

These tests fail due to combinations of the above issues:

| Test | Primary Issues |
|------|----------------|
| `test_profile_all_columns` | std_dev + distinct count |
| `test_profile_specific_columns` | std_dev formula |
| `test_completeness_partial` | NULL counting edge case |
| `test_numeric_with_nulls` | std_dev with NULLs |
| `test_single_row` | std_dev undefined (0/0) |
| `test_mixed_types` | Type inference + formatting |

**Single Row Edge Case:**

Standard deviation of a single value is mathematically undefined (sample) or 0 (population):
- `STDDEV_SAMP([42])` → NULL or NaN
- `STDDEV_POP([42])` → 0

**Options to Bridge:**

Fixing the underlying issues (std_dev, distinct count, output format) will resolve most profile test failures.

---

## Recommended Fixes

| Priority | Fix | Impact | Effort | Tests Fixed |
|----------|-----|--------|--------|-------------|
| **High** | Parse JSON in comparison utility | 2 tests | Low | histogram, data_type |
| **High** | Add STDDEV_POP configuration option | 4+ tests | Low | std_dev, profile tests |
| **Medium** | Increase tolerances for approximate metrics | 5+ tests | Low | approx_count, quantiles, entropy |
| **Medium** | Use exact COUNT DISTINCT for small datasets | 2 tests | Medium | approx_count_distinct, distinct_values |
| **Low** | Implement numerical stability improvements | 3 tests | High | entropy, mutual_info |

### Implementation Roadmap

**Phase 1: Quick Wins (1-2 days)**
1. Add JSON parsing to test comparison utilities
2. Add configuration option for standard deviation formula
3. Update test tolerances for approximate metrics

**Phase 2: Standardization (1 week)**
1. Implement exact distinct count fallback for small datasets
2. Standardize output formats across engines
3. Document expected variances in analyzer docstrings

**Phase 3: Long-term (future)**
1. Implement numerically stable entropy calculations
2. Add configurable interpolation methods for quantiles
3. Create comprehensive engine compatibility matrix

---

## Acceptance Criteria

Tests can be categorized into three resolution states:

### 1. Fixed
Implementation changed to match Spark behavior:
- Standard deviation formula alignment
- Output format standardization
- Exact counts for small datasets

### 2. Relaxed
Tolerance increased with documented reasoning:
- Approximate count distinct (inherent HLL variance)
- Quantile calculations (interpolation method differences)
- Entropy calculations (floating-point precision limits)

### 3. Skipped
Marked as known difference with `pytest.mark.xfail`:
- Engine-specific features not portable
- Fundamental algorithmic differences

---

## Appendix: Test Failure Details

### Full List of Failing Tests

1. `test_standard_deviation` - Sample vs population formula
2. `test_approx_count_distinct` - HyperLogLog implementation
3. `test_approx_quantile_median` - Interpolation method
4. `test_approx_quantile_quartiles` - Interpolation method
5. `test_entropy_uniform` - Floating-point precision
6. `test_mutual_information` - Floating-point precision
7. `test_has_entropy` - Floating-point precision
8. `test_histogram` - JSON vs Map output
9. `test_data_type` - JSON vs Map output
10. `test_profile_all_columns` - Combined issues
11. `test_profile_specific_columns` - Combined issues
12. `test_completeness_partial` - NULL handling
13. `test_numeric_with_nulls` - std_dev with NULLs
14. `test_single_row` - Edge case handling
15. `test_mixed_types` - Type inference
16. `test_numeric_statistics` - std_dev formula
17. `test_all_basic_analyzers` - std_dev formula
18. `test_distinct_values` - Approximate count

### Reference: Standard Deviation Formulas

**Population Standard Deviation:**
```
σ = √(Σ(xi - μ)² / N)
```

**Sample Standard Deviation:**
```
s = √(Σ(xi - x̄)² / (N-1))
```

**Relationship:**
```
s = σ × √(N / (N-1))
```

For N=5: s ≈ 1.118 × σ

---

## Conclusion

The 18 test failures between Spark and DuckDB engines stem from five root causes, all of which have viable resolution paths. The recommended approach prioritizes:

1. Quick fixes that achieve parity (output format, std_dev config)
2. Appropriate tolerance adjustments for probabilistic algorithms
3. Documentation of inherent differences

With the high-priority fixes implemented, the test suite should achieve >90% parity. The remaining differences reflect fundamental algorithmic choices that should be documented rather than hidden.
