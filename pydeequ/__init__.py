# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
"""Placeholder docstrings"""



from pyspark.sql import SparkSession
from pydeequ.profiles import ColumnProfilerRunner
from pydeequ.analyzers import AnalysisRunner
from pydeequ.checks import Check, CheckLevel

deequ_maven_coord = "com.amazon.deequ:deequ:1.1.0_spark-2.4-scala-2.11"
# deequ_maven_coord = "com.amazon.deequ:deequ:1.1.0_spark-3.0-scala-2.12"
f2j_maven_coord = "net.sourceforge.f2j:arpack_combined_all"

class PyDeequSession:
    """
    For interacting with PyDeequ Modules at the "Runner" Level
    """

    def __init__(self, spark_session: SparkSession):
        self._spark_session = spark_session
        self._sc = spark_session.sparkContext
        self._jvm = spark_session._jvm

    def createColumnProfileRunner(self):
        return ColumnProfilerRunner(self._spark_session)

    def createAnalysisRunner(self):
        return AnalysisRunner(self._spark_session)

    def createCheck(self, level: CheckLevel, description: str, constraints=None):
        return Check(self._spark_session, level, description, constraints)