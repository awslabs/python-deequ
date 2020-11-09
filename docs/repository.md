# Metrics Repository

Here are the current supported functionalities of Metrics Repository.

| Class               | Method                                          | Status |
|---------------------|-------------------------------------------------|:------:|
| InMemoryMetricsRepository      | InMemoryMetricsRepository(spark_session)                               | Done   |
|  | helper_metrics_file(spark_session, filename) | Done |
|  | load() | Done |
|  | withTagValues(tagValues) | Done |
|  | forAnalyzers(analyzers) | Done |
|  | before(dateTime) | Done |
|  | after(dateTime) | Done |
|  | getSuccessMetricsAsJson(withTags) | Done |
|  | getSuccessMetricsAsDataFrame(withTags) | Done |
| FileSystemMetricsRepository      | InMemoryMetricsRepository(spark_session)                               | Done   |
|  | helper_metrics_file(spark_session, filename) | Done |
|  | load() | Done |
|  | withTagValues(tagValues) | Done |
|  | forAnalyzers(analyzers) | Done |
|  | before(dateTime) | Done |
|  | after(dateTime) | Done |
|  | getSuccessMetricsAsJson(withTags) | Done |
|  | getSuccessMetricsAsDataFrame(withTags) | Done |
| ResultKey | ResultKey(spark_session, dataSetDate) | Done |
|  | current_milli_time() | Done |
