# -*- coding: utf-8 -*-
"""
Spark helper functions for PyDeequ v2.

This module provides helper functions for working with Spark Connect,
including compatibility shims for different Spark versions.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from google.protobuf import any_pb2

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.connect.plan import LogicalPlan


def dataframe_from_plan(plan: "LogicalPlan", session: "SparkSession") -> "DataFrame":
    """
    Create a DataFrame from a LogicalPlan, handling Spark version differences.

    Spark 3.x uses DataFrame.withPlan(plan, session)
    Spark 4.x uses DataFrame(plan, session)

    Args:
        plan: LogicalPlan to create DataFrame from
        session: SparkSession

    Returns:
        DataFrame wrapping the plan
    """
    from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame

    if hasattr(ConnectDataFrame, "withPlan"):
        # Spark 3.x
        return ConnectDataFrame.withPlan(plan, session=session)

    # Spark 4.x
    return ConnectDataFrame(plan, session)


def create_deequ_plan(extension: any_pb2.Any) -> "LogicalPlan":
    """
    Create a LogicalPlan subclass for Deequ that properly integrates with PySpark.

    We dynamically import and subclass LogicalPlan to avoid import issues
    when Spark Connect is not available.

    Args:
        extension: Protobuf Any message containing the Deequ operation

    Returns:
        LogicalPlan instance for the Deequ operation
    """
    import pyspark.sql.connect.proto as spark_proto
    from pyspark.sql.connect.plan import LogicalPlan

    class _DeequExtensionPlan(LogicalPlan):
        """
        Custom LogicalPlan for Deequ operations via Spark Connect.

        This plan wraps our protobuf message as a Relation extension,
        which is sent to the server and handled by DeequRelationPlugin.
        """

        def __init__(self, ext: any_pb2.Any):
            # Pass None as child - this is a leaf node
            super().__init__(child=None)
            self._extension = ext

        def plan(self, session) -> spark_proto.Relation:
            """Return the Relation proto for this plan."""
            rel = self._create_proto_relation()
            rel.extension.CopyFrom(self._extension)
            return rel

        def __repr__(self) -> str:
            return "DeequExtensionPlan"

    return _DeequExtensionPlan(extension)


__all__ = [
    "dataframe_from_plan",
    "create_deequ_plan",
]
