# Checks 

Here are the current supported functionalities of Checks. 

| Class               | Method                                          | Status |
|---------------------|-------------------------------------------------|:------:|
| Check      | Check(spark_session, level, description, constraints)                               | Done   |
|  | addConstraint(constraint) | Not Planned |
|  | addFilterableContstraint(creationFunc) | Not Planned |
|  | hasSize(assertion) | Done |
|  | isComplete(column) | Done |
|  | hasCompleteness(column, assertion) | Done |
|  | areComplete(columns) | Done |
|  | haveCompleteness(columns, assertion) | Done |
|  | areAnyComplete(columns) | Done |
|  | haveAnyCompleteness(columns, assertion) | Done |
|  | isUnique(column) | Done |
|  | isPrimaryKey(column, *columns) | Not Implemented |
|  | hasUniqueness(columns, assertion) | Done |
|  | hasDistinctness(columns, assertion) | Done |
|  | hasUniqueValueRatio(columns, assertion) | Done |
|  | hasNumberOfDistinctValues(column, assertion, binningUdf, maxBins) | Done |
|  | hasHistogramValues(column, assertion, binningUdf, maxBins) | Done |
|  | kllSketchSatisfies(column, assertion, kllParameters) | Done |
|  | hasEntropy(column, assertion) | Done |
|  | hasMutualInformation(columnA, columnB, assertion) | Done |
|  | hasApproxQuantile(column, quantile, assertion) | Done |
|  | hasMinLength(column, assertion) | Done |
|  | hasMaxLength(column, assertion) | Done |
|  | hasMin(column, assertion) | Done |
|  | hasMax(column, assertion) | Done |
|  | hasMean(column, assertion) | Done |
|  | hasSum(column, assertion) | Done |
|  | hasStandardDeviation(column, assertion) | Done |
|  | hasApproxCountDistinct(column, assertion) | Done |
|  | hasCorrelation(columnA, columnB, assertion) | Done |
|  | satisfies(columnCondition, constraintName, assertion) | Done |
|  | hasPattern(column, pattern, assertion, name) | Done |
|  | containsCreditCardNumber(column, assertion) | Done |
|  | containsEmail(column, assertion) | Done |
|  | containsURL(column, assertion) | Done |
|  | containsSocialSecurityNumber(column, assertion) | Done |
|  | hasDataType(column, datatype, assertion) | Done |
|  | isNonNegative(column, assertion) | Done |
|  | isPositive(column, assertion) | Done |
|  | isLessThan(columnA, columnB, assertion) | Done |
|  | isLessThanOrEqualTo(columnA, columnB) | Done |
|  | isGreaterThan(columnA, columnB, assertion) | Done |
|  | isGreaterThanOrEqualTo(columnA, columnB) | Done |
|  | isContainedIn(column, allowed_values) | Done |
|  | evaluate(context) | Not Implemented |
|  | requiredAnalyzers() | Not Planned |
|  | 

