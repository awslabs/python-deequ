# -*- coding: utf-8 -*-
from enum import Enum

######################## BASE ANALYZER CLASS #######################


class _AnomalyObject:
    """AnomalyDetection object init to pass and accumulate the anomaly strategies of the run"""

    def _set_jvm(self, jvm):
        self._jvm = jvm
        return self

    @property
    def _deequAnomalies(self):
        if self._jvm:
            return self._jvm.com.amazon.deequ.anomalydetection
        raise AttributeError(
            "JVM not set, please run _set_jvm() method first."
        )  # TODO: Test that this exception gets raised


######################## INDIVIDUAL ANOMALY STRATEGIES  ########################


class RelativeRateOfChangeStrategy(_AnomalyObject):
    """
    Detects anomalies based on the values' rate of change.
    For example RelativeRateOfChangeStrategy(.9, 1.1, 1) calculates the first discrete difference,
    and if some point's value changes by more than 10.0 percent in one timestep it flags it as an anomaly.

    :param float maxRateDecrease: Lower bound of accepted relative change (as new value / old value).
    :param float maxRateIncrease: Upper bound of accepted relative change (as new value / old value).
    :param int order: Order of the calculated difference.
            Defaulted to 1, it calculates the difference between two consecutive
            values. The order of the difference can be set manually. If it is set to 0, this strategy acts
            like the SimpleThresholdStrategy class.
    """

    def __init__(self, maxRateDecrease=None, maxRateIncrease=None, order=1):

        self.maxRateDecrease = maxRateDecrease
        self.maxRateIncrease = maxRateIncrease
        self.order = order

    @property
    def _anomaly_jvm(self):
        return self._deequAnomalies.RelativeRateOfChangeStrategy(
            self._jvm.scala.Option.apply(self.maxRateDecrease),
            self._jvm.scala.Option.apply(self.maxRateIncrease),
            self.order,
        )


class AbsoluteChangeStrategy(_AnomalyObject):
    """
    Detects anomalies based on the values' absolute change.
    AbsoluteChangeStrategy(-10.0, 10.0, 1) for example calculates the first discrete difference
    and if some point's value changes by more than 10.0 in one timestep, it flags it as an anomaly.

    :param float maxRateDecrease: Upper bound of accepted decrease (lower bound of increase).
    :param float maxRateIncrease: Upper bound of accepted growth.
    :param int order:  Order of the calculated difference.
            Defaulted to 1, it calculates the difference between two consecutive
            values. The order of the difference can be set manually. If it is set to 0, this strategy acts
            like the SimpleThresholdStrategy class.
    """

    def __init__(self, maxRateDecrease=None, maxRateIncrease=None, order=1):

        self.maxRateDecrease = maxRateDecrease
        self.maxRateIncrease = maxRateIncrease
        self.order = order

    @property
    def _anomaly_jvm(self):
        return self._deequAnomalies.AbsoluteChangeStrategy(
            self._jvm.scala.Option.apply(self.maxRateDecrease),
            self._jvm.scala.Option.apply(self.maxRateIncrease),
            self.order,
        )


class SimpleThresholdStrategy(_AnomalyObject):
    """A simple anomaly detection strategy that checks if values are  in a specified range.

    :param float lowerBound: Lower bound of accepted range of values
    :param float upperBound: Upper bound of accepted range of values
    """

    def __init__(self, lowerBound, upperBound):

        self.lowerBound = float(lowerBound)
        self.upperBound = float(upperBound)

    @property
    def _anomaly_jvm(self):
        return self._deequAnomalies.SimpleThresholdStrategy(self.lowerBound, self.upperBound)


class RateOfChangeStrategy(_AnomalyObject):
    """
    @Deprecated
    The old RateOfChangeStrategy actually detects absolute changes so it has been migrated to use the
    AbsoluteChangeStrategy class. Use RelativeRateOfChangeStrategy if you want to detect relative changes to the
    previous values.

    """

    def __init__(self, maxRateDecrease=None, maxRateIncrease=None, order=1):
        # self.maxRateDecrease = maxRateDecrease
        # self.maxRateIncrease = maxRateIncrease
        # self.order = order
        pass

    @property
    def _anomaly_jvm(self):
        raise DeprecationWarning("use AbsoluteChangeStrategy instead which describes the strategy more accurately")


class OnlineNormalStrategy(_AnomalyObject):
    """
    Detects anomalies based on the running mean and standard deviation. Anomalies can be excluded from the computation to
    not affect the calculated mean/ standard deviation. This strategy assumes that the data is normally distributed.

    :param float lowerDeviationFactor: Detects anomalies if they are smaller than the mean - lowerDeviationFactor *stdDev
    :param float upperDeviationFactor: Detect anomalies if they are bigger than mean + upperDeviationFactor * stDev
    :param float ignoreStartPercentage: Percentage of data points after start in which no anomalies should be detected
                        (mean and stdDev are probably not representative before).
    :param boolean ignoreAnomalies: If set to true, ignores anomalous points in mean and variance calculation
    """

    def __init__(
        self,
        lowerDeviationFactor=3.0,
        upperDeviationFactor=3.0,
        ignoreStartPercentage=0.1,
        ignoreAnomalies=True,
    ):
        self.lowerDeviationFactor = lowerDeviationFactor
        self.upperDeviationFactor = upperDeviationFactor
        self.ignoreStartPercentage = ignoreStartPercentage
        self.ignoreAnomalies = ignoreAnomalies

    # TODO: Consider adding OnlineCalculationResult
    @property
    def _anomaly_jvm(self):
        return self._deequAnomalies.OnlineNormalStrategy(
            self._jvm.scala.Option.apply(self.lowerDeviationFactor),
            self._jvm.scala.Option.apply(self.upperDeviationFactor),
            self.ignoreStartPercentage,
            self.ignoreAnomalies,
        )


class BatchNormalStrategy(_AnomalyObject):
    """
    Detects anomalies based on the mean and standard deviation of all available values.
    The strategy assumes that the data is normally distributed.

    :param float lowerDeviationFactor: Detect anomalies if they are smaller than mean - lowerDeviationFactor * stdDev
    :param float upperDeviationFactor: Detect anomalies if they are bigger than mean + upperDeviationFactor * stdDev
    :param boolean includeInterval: Discerns whether or not the values inside the detection interval should
                be included in the calculation of the mean / stdDev.
    """

    def __init__(self, lowerDeviationFactor=3.0, upperDeviationFactor=3.0, includeInterval=False):
        self.lowerDeviationFactor = lowerDeviationFactor
        self.upperDeviationFactor = upperDeviationFactor
        self.includeInterval = includeInterval

    @property
    def _anomaly_jvm(self):
        return self._deequAnomalies.BatchNormalStrategy(
            self._jvm.scala.Option.apply(self.lowerDeviationFactor),
            self._jvm.scala.Option.apply(self.upperDeviationFactor),
            self.includeInterval,
        )


######################## HoltWinters  ########################


class MetricInterval(Enum):
    """Metric Interval is how often the metric of interest is computed (e.g. daily)."""

    Daily = "Daily"
    Monthly = "Monthly"

    def _get_java_object(self, jvm):
        if self == MetricInterval.Daily:
            return getattr(
                jvm.com.amazon.deequ.anomalydetection.seasonal.HoltWinters,
                "MetricInterval$",
            )().Daily()

        if self == MetricInterval.Monthly:
            return getattr(
                jvm.com.amazon.deequ.anomalydetection.seasonal.HoltWinters,
                "MetricInterval$",
            )().Monthly()
        raise ValueError("Invalid value for MetricInterval Enum")


class SeriesSeasonality(Enum):
    """
    SeriesSeasonality is the expected metric seasonality which defines the longest cycle in series. This is also
    referred to as periodicity.
    """

    Weekly = "Weekly"
    Yearly = "Yearly"

    def _get_java_object(self, jvm):
        if self == SeriesSeasonality.Weekly:
            return getattr(
                jvm.com.amazon.deequ.anomalydetection.seasonal.HoltWinters,
                "SeriesSeasonality$",
            )().Weekly()

        if self == SeriesSeasonality.Yearly:
            return getattr(
                jvm.com.amazon.deequ.anomalydetection.seasonal.HoltWinters,
                "SeriesSeasonality$",
            )().Yearly()
        raise ValueError("Invalid value for MetricInterval Enum")


class HoltWinters(_AnomalyObject):
    """
    Detects anomalies based on the additive Holt-Winters model.
    For example if a metric is produced daily and repeats itself every Monday,
    then the model should be created with a Daily metric interval and a Weekly seasonality parameter.
    To implement two cycles of data a minimum of 15 entries must be given for SeriesSeasonality.Weekly,
    MetricInterval.Daily, and 25 for SeriesSeasonality.Yearly, MetricInterval.Monthly.

    :param MetricInterval metricsInterval: How often a metric is available
    :param SeriesSeasonality seasonality: Cycle length (or periodicity) of the metric
    """

    def __init__(self, metricsInterval: MetricInterval, seasonality: SeriesSeasonality):
        self.metricsInterval = metricsInterval
        self.seasonality = seasonality

    @property
    def _anomaly_jvm(self):
        return self._deequAnomalies.seasonal.HoltWinters(
            self.metricsInterval._get_java_object(self._jvm), self.seasonality._get_java_object(self._jvm)
        )


######################## Anomaly Detector  ########################


class _AnomalyDetector(_AnomalyObject):

    """
    Detects anomalies in a time series give a strategy. This class is mainly responsible for
    the pre processing of the given time series.

    :param strategy:An implementation of AnomalyDetectionStrategy that needs preprocessed values.
    """

    def __init__(self, strategy):
        raise NotImplementedError("AnomalyDetector has not been implemented yet!")


class _DataPoint:
    def __init__(self, time, metricValue):
        raise NotImplementedError("DataPoint has not been implemented yet!")


######################## Detection Result  ########################


class _Anomaly:
    def __init__(self, value, confidence, detail):
        raise NotImplementedError("Anomaly has not been implemented yet!")
