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
| CustomSql | CustomSql(expression, disambiguator) | Done| 
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


 