# Profiles 

Here are the current supported functionalities of Profiles. 

| Class               | Method                                          | Status |
|---------------------|-------------------------------------------------|:------:|
| ColumnProfilerRunner      | ColumnPrpfilerRunner(spark_session)                               | Done   |
|  | onData(data)                           | Done   |
|  | run(data, restrictToColumns, lowCardinalityHistogramThreshold, printStatusUpdates, cacheInputs, fileOutputOptions, metricsRepositoryOptions, kllParameters, predefinedTypes) |  |
| ColumnProfilerRunBuilder | ColumnProfilerRunBuilder(spark_session, data) | Done |
|  | run() | Done |
|  | printStatusUpdates(print_status_updates) | Done |
|  | cacheInputs(cache_inputs) | Done |
|  | withLowCardinalityHistogramThreshold(low_cardinality_histogram_threshold) | Done |
|  | restrictToColumns(restrict_to_columns) |  |
|  | withKLLProfiling() | Done |
|  | setKLLParameters(kllParameters) | Done |
|  | useRepository(repository) | Done |
|  | saveOrAppendResult(resultKey) | Done |
|  | useSparkSession |  |
| ColumnProfilesBuilder | ColumnProfilesBuilder(spark_session) | Done |
|  | property: profiles | Done |
|  | property: numRecords | Done |
| StandardColumnProfile | StandardColumnProfile(spark_session, column, java_column_profile) | Done |
| NumericColumnProfile | NumericColumnProfile(spark_session, column, java_column_profile) | Done |
