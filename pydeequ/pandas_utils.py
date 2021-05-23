# -*- coding: utf-8 -*-
"""
Utility methods to help PyDeequ interface with Pandas
"""
import warnings
from typing import Union

from pandas import DataFrame as pandasDF
from pyspark.sql import DataFrame as sparkDF
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


def ensure_pyspark_df(spark_session: SparkSession, df: Union[pandasDF, sparkDF]):
    """Method for checking dataframe type for each onData() call from a RunBuilder."""
    if not isinstance(df, sparkDF):
        warnings.warn(
            "WARNING: You passed in a Pandas DF, so we will be using our experimental utility to "
            "convert it to a PySpark DF."
        )
        df = PandasConverter.pandasDF_to_pysparkDF(spark_session, df)
    return df


class PandasConverter:
    """
    Pandas Functor to interface between Pandas and PyDeequ

    CONVERSIONS:
    Pandas DF DataType | PySpark DF DataType
    int32              | IntegerType()
    int64              | LongType()
    float64            | FloatType()
    bool               | BooleanType()
    datetime64[ns]     | DateType()
    object             | StringType()

    NOTE: LIMITED FUNCTIONALITY. Handle this prior and pass a Spark DF in if you want to customize
    - NaN columns get mapped to floats
    - Python lists & dicts get mapped to String
    - The following are unsupported automatic conversions: "TimestampType", "DecimalType", "DoubleType", "ByteType",
        "ShortType", "ArrayType", "MapType"
    """

    p2s_map = {
        "datetime64[ns]": DateType(),
        "int64": LongType(),
        "int32": IntegerType(),
        "float64": FloatType(),
        "bool": BooleanType(),
    }

    @classmethod
    def _define_structure(cls, string, format_type):
        typo = cls.p2s_map.get(str(format_type), StringType())
        return StructField(string, typo)

    @classmethod
    def pandasDF_to_pysparkDF(cls, spark_session: SparkSession, pandas_df: pandasDF):
        columns = list(pandas_df.columns)
        types = list(pandas_df.dtypes)
        struct_list = [cls._define_structure(column, typo) for column, typo in zip(columns, types)]

        p_schema = StructType(struct_list)
        return spark_session.createDataFrame(pandas_df, p_schema)

    @classmethod
    def pysparkDF_to_pandasDF(cls, spark_df: sparkDF):
        return spark_df.toPandas()
