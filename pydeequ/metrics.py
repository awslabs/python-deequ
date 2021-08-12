# -*- coding: utf-8 -*-
"""
Metrics file for the various metrics that Deequ handles
"""
from pyspark.sql import SparkSession


class BucketValue:
    def __init__(self, bucket_value):
        self._bucketValue = bucket_value

    def __str__(self):
        return str(self._bucketValue)

    @property
    def lowValue(self):
        return float(self._bucketValue.lowValue())

    @property
    def highValue(self):
        return float(self._bucketValue.highValue())

    @property
    def count(self):
        return int(self._bucketValue.count())


class BucketDistribution:
    """
    Bucket Distribution for KLL
    """

    def __init__(self, spark_session: SparkSession, kllResult):
        self._spark_session = spark_session
        self._kllResult = kllResult
        self._buckets = [
            BucketValue(self._kllResult.buckets().apply(b)) for b in range(self._kllResult.buckets().size())
        ]
        self._parameters = [
            float(self._kllResult.parameters().apply(p)) for p in range(self._kllResult.parameters().size())
        ]
        self._data = [list(d) for d in self._kllResult.data()]

    def __str__(self):
        # TODO: Reformat
        return str(self._kllResult)

    @property
    def buckets(self):
        return self._buckets

    @property
    def parameters(self):
        return self._parameters

    @property
    def data(self):
        return self._data

    def computePercentiles(self):
        """
        Compute the percentiles.
        """
        return list(self._kllResult.computePercentiles())

    def apply(self, key: int):
        """
        Get relevant bucketValue with index of bucket.

        :param int key: index of bucket
        :return BucketValue: The metrics for the bucket
        """
        return BucketValue(self._kllResult.apply(key))

    @property
    def argmax(self):
        """
        Find the index of bucket which contains the most items.

        :return: The index of bucket which contains the most items.
        """
        return self._kllResult.argmax()
