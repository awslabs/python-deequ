# Analyzers 

Here are the current supported functionalities of Analyzers. 

| Class               | Method                                          | Status |
|---------------------|-------------------------------------------------|:------:|
| AnalysisRunner      | onData(DataFrame)                               | Done   |
| AnalysisRunBuilder  | addAnalyzer(analyzer)                           | Done   |
|                     | run()                                           | Done   |
|                     | useRepository(repository)                       | Done   |
|                     | saveOrAppendResult(resultKey)                   | Done   |
| ApproxCountDistinct | ApproxCountDistinct(column)                     | Done   |
| ApproxQuantile      | ApproxQuantile(column, quantile, relativeError) | Done       |
| ApproxQuantiles     | ApproxQuantiles(column, quantiles)           |  Done      |
| Completeness          | Completeness(column)          |      Done     |
| Compliance | Compliance(instance, predicate) | Done|
| Correlation | Correlation(column1, column2) | Done| 
| CountDistinct | CountDistinct(columns) | Done| 
| Datatype | Datatype(column) | Done| 
| Distinctness | Distinctness(columns) | Done| 
| Entropy | Entropy(column) | Done| 
| Histogram | Histogram(column, binningUdf, maxDetailBins) | Done|
| KLLParameters | KLLParameters(spark_session, sketchSize, shrinkingFactor, numberOfBuckets) | Done|
| KLLSketch | KLLSketch(column, kllParameters) | Done | 
| Histogram_maxBins | Histogram_maxBins(column, binningUdf, maxDetailBins) | Done | 
| Maximum | Maximum(column) | Done| 
| MaxLength | MaxLength(column) | Done| 
| Mean | Mean(column) | Done| 
| Minimum | Minimum(column) | Done| 
| MinLength | MinLength(column) | Done| 
| MutualInformation | MutualInformation(columns) | Done| 
| StandardDeviation | StandardDeviation(column) | Done| 
| Sum | Sum(column) | Done| 
| Uniqueness | Uniqueness(columns) | Done| 
| UniqueValueRatio | UniqueValueRatio(columns) | Done|
| AnalyzerContext | successMetricsAsDataFrame(spark_session, analyzerContext) | Done |
|   | successMetricsAsJson(spark_session, analyzerContext) | Done |
| Distance | Distance(spark_session).categoricalDistance(distribution1, distribution2, method) | Done |


## Distance (categorical feature drift)

`Distance` wraps Deequ's `com.amazon.deequ.analyzers.Distance` object to compute
the distance (feature drift) between two categorical distributions, using either
the L-infinity or chi-squared method.

Unlike the other analyzers, Deequ's `Distance` is a static object rather than an
`Analyzer`, so it is *not* added through `addAnalyzer(...)`. Instead, call it
directly with two distributions, each a `dict` of `{category: count}` (for
example, the output of the `Histogram` analyzer).

```python
from pydeequ.analyzers import Distance, CategoricalDistanceMethod

distance = Distance(spark)

# Two distributions as {category: count}
reference = {"a": 10, "b": 20, "c": 30}
current = {"a": 11, "b": 20, "c": 29}

# L-infinity distance (default)
linf = distance.categoricalDistance(reference, current)

# Chi-squared distance
chi = distance.categoricalDistance(
    reference, current, method=CategoricalDistanceMethod.Chisquare
)
```

Only the categorical path is supported. The numerical path
(`numericalDistance`) requires a JVM `QuantileNonSample[Double]` and is out of
scope (see issue #164).
 