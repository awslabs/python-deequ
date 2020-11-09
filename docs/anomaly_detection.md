# Anomaly Detection 

Here are the current supported functionalities of Anomaly Detection. 

| Class               | Method                                          | Status |
|---------------------|-------------------------------------------------|:------:|
| RelativeRateOfChangeStrategy | RelativeRateOfChangeStrategy(maxRateDecrease, maxRateIncrease, order) | Done |
| AbsoluteChangeStrategy  | AbsoluteChangeStrategy(maxRateDecrease, maxRateIncrease, order) | Done |
| SimpleThresholdStrategy | SimpleThresholdStrategy(lowerBound, upperBound) | Done |
| OnlineNormalStrategy | OnlineNormalStrategy(lowerDeviationFactor, upperDeviationFactor, ignoreStartPercentage, ignoreAnomalies) | Done |
| BatchNormalStrategy | BatchNormalStrategy(lowerDeviationFactor, upperDeviationFactor, includeInterval) | Done |
| MetricInterval(Enum) | ['Daily','Monthly'] | Done |
| SeriesSeasonality(Enum) | ['Weekly','Yearly'] | Done |
| HoltWinters | HoltWinters(metricsInterval, seasonality) | Done |
