from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class EmptySpec(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ColumnAnalyzerSpec(_message.Message):
    __slots__ = ("column",)
    COLUMN_FIELD_NUMBER: _ClassVar[int]
    column: str
    def __init__(self, column: _Optional[str] = ...) -> None: ...

class ColumnsAnalyzerSpec(_message.Message):
    __slots__ = ("columns",)
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    columns: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, columns: _Optional[_Iterable[str]] = ...) -> None: ...

class PairColumnsAnalyzerSpec(_message.Message):
    __slots__ = ("column_a", "column_b")
    COLUMN_A_FIELD_NUMBER: _ClassVar[int]
    COLUMN_B_FIELD_NUMBER: _ClassVar[int]
    column_a: str
    column_b: str
    def __init__(self, column_a: _Optional[str] = ..., column_b: _Optional[str] = ...) -> None: ...

class ApproxQuantileSpec(_message.Message):
    __slots__ = ("column", "quantile", "relative_error")
    COLUMN_FIELD_NUMBER: _ClassVar[int]
    QUANTILE_FIELD_NUMBER: _ClassVar[int]
    RELATIVE_ERROR_FIELD_NUMBER: _ClassVar[int]
    column: str
    quantile: float
    relative_error: float
    def __init__(self, column: _Optional[str] = ..., quantile: _Optional[float] = ..., relative_error: _Optional[float] = ...) -> None: ...

class ApproxQuantilesSpec(_message.Message):
    __slots__ = ("column", "quantiles", "relative_error")
    COLUMN_FIELD_NUMBER: _ClassVar[int]
    QUANTILES_FIELD_NUMBER: _ClassVar[int]
    RELATIVE_ERROR_FIELD_NUMBER: _ClassVar[int]
    column: str
    quantiles: _containers.RepeatedScalarFieldContainer[float]
    relative_error: float
    def __init__(self, column: _Optional[str] = ..., quantiles: _Optional[_Iterable[float]] = ..., relative_error: _Optional[float] = ...) -> None: ...

class HistogramSpec(_message.Message):
    __slots__ = ("column", "max_detail_bins")
    COLUMN_FIELD_NUMBER: _ClassVar[int]
    MAX_DETAIL_BINS_FIELD_NUMBER: _ClassVar[int]
    column: str
    max_detail_bins: int
    def __init__(self, column: _Optional[str] = ..., max_detail_bins: _Optional[int] = ...) -> None: ...

class ComplianceAnalyzerSpec(_message.Message):
    __slots__ = ("instance", "predicate")
    INSTANCE_FIELD_NUMBER: _ClassVar[int]
    PREDICATE_FIELD_NUMBER: _ClassVar[int]
    instance: str
    predicate: str
    def __init__(self, instance: _Optional[str] = ..., predicate: _Optional[str] = ...) -> None: ...

class PatternMatchSpec(_message.Message):
    __slots__ = ("column", "pattern")
    COLUMN_FIELD_NUMBER: _ClassVar[int]
    PATTERN_FIELD_NUMBER: _ClassVar[int]
    column: str
    pattern: str
    def __init__(self, column: _Optional[str] = ..., pattern: _Optional[str] = ...) -> None: ...

class Analyzer(_message.Message):
    __slots__ = ("where", "size", "completeness", "mean", "sum", "standard_deviation", "minimum", "maximum", "min_length", "max_length", "approx_count_distinct", "entropy", "data_type", "uniqueness", "distinctness", "unique_value_ratio", "count_distinct", "mutual_information", "correlation", "approx_quantile", "approx_quantiles", "histogram", "compliance", "pattern_match")
    WHERE_FIELD_NUMBER: _ClassVar[int]
    SIZE_FIELD_NUMBER: _ClassVar[int]
    COMPLETENESS_FIELD_NUMBER: _ClassVar[int]
    MEAN_FIELD_NUMBER: _ClassVar[int]
    SUM_FIELD_NUMBER: _ClassVar[int]
    STANDARD_DEVIATION_FIELD_NUMBER: _ClassVar[int]
    MINIMUM_FIELD_NUMBER: _ClassVar[int]
    MAXIMUM_FIELD_NUMBER: _ClassVar[int]
    MIN_LENGTH_FIELD_NUMBER: _ClassVar[int]
    MAX_LENGTH_FIELD_NUMBER: _ClassVar[int]
    APPROX_COUNT_DISTINCT_FIELD_NUMBER: _ClassVar[int]
    ENTROPY_FIELD_NUMBER: _ClassVar[int]
    DATA_TYPE_FIELD_NUMBER: _ClassVar[int]
    UNIQUENESS_FIELD_NUMBER: _ClassVar[int]
    DISTINCTNESS_FIELD_NUMBER: _ClassVar[int]
    UNIQUE_VALUE_RATIO_FIELD_NUMBER: _ClassVar[int]
    COUNT_DISTINCT_FIELD_NUMBER: _ClassVar[int]
    MUTUAL_INFORMATION_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_FIELD_NUMBER: _ClassVar[int]
    APPROX_QUANTILE_FIELD_NUMBER: _ClassVar[int]
    APPROX_QUANTILES_FIELD_NUMBER: _ClassVar[int]
    HISTOGRAM_FIELD_NUMBER: _ClassVar[int]
    COMPLIANCE_FIELD_NUMBER: _ClassVar[int]
    PATTERN_MATCH_FIELD_NUMBER: _ClassVar[int]
    where: str
    size: EmptySpec
    completeness: ColumnAnalyzerSpec
    mean: ColumnAnalyzerSpec
    sum: ColumnAnalyzerSpec
    standard_deviation: ColumnAnalyzerSpec
    minimum: ColumnAnalyzerSpec
    maximum: ColumnAnalyzerSpec
    min_length: ColumnAnalyzerSpec
    max_length: ColumnAnalyzerSpec
    approx_count_distinct: ColumnAnalyzerSpec
    entropy: ColumnAnalyzerSpec
    data_type: ColumnAnalyzerSpec
    uniqueness: ColumnsAnalyzerSpec
    distinctness: ColumnsAnalyzerSpec
    unique_value_ratio: ColumnsAnalyzerSpec
    count_distinct: ColumnsAnalyzerSpec
    mutual_information: ColumnsAnalyzerSpec
    correlation: PairColumnsAnalyzerSpec
    approx_quantile: ApproxQuantileSpec
    approx_quantiles: ApproxQuantilesSpec
    histogram: HistogramSpec
    compliance: ComplianceAnalyzerSpec
    pattern_match: PatternMatchSpec
    def __init__(self, where: _Optional[str] = ..., size: _Optional[_Union[EmptySpec, _Mapping]] = ..., completeness: _Optional[_Union[ColumnAnalyzerSpec, _Mapping]] = ..., mean: _Optional[_Union[ColumnAnalyzerSpec, _Mapping]] = ..., sum: _Optional[_Union[ColumnAnalyzerSpec, _Mapping]] = ..., standard_deviation: _Optional[_Union[ColumnAnalyzerSpec, _Mapping]] = ..., minimum: _Optional[_Union[ColumnAnalyzerSpec, _Mapping]] = ..., maximum: _Optional[_Union[ColumnAnalyzerSpec, _Mapping]] = ..., min_length: _Optional[_Union[ColumnAnalyzerSpec, _Mapping]] = ..., max_length: _Optional[_Union[ColumnAnalyzerSpec, _Mapping]] = ..., approx_count_distinct: _Optional[_Union[ColumnAnalyzerSpec, _Mapping]] = ..., entropy: _Optional[_Union[ColumnAnalyzerSpec, _Mapping]] = ..., data_type: _Optional[_Union[ColumnAnalyzerSpec, _Mapping]] = ..., uniqueness: _Optional[_Union[ColumnsAnalyzerSpec, _Mapping]] = ..., distinctness: _Optional[_Union[ColumnsAnalyzerSpec, _Mapping]] = ..., unique_value_ratio: _Optional[_Union[ColumnsAnalyzerSpec, _Mapping]] = ..., count_distinct: _Optional[_Union[ColumnsAnalyzerSpec, _Mapping]] = ..., mutual_information: _Optional[_Union[ColumnsAnalyzerSpec, _Mapping]] = ..., correlation: _Optional[_Union[PairColumnsAnalyzerSpec, _Mapping]] = ..., approx_quantile: _Optional[_Union[ApproxQuantileSpec, _Mapping]] = ..., approx_quantiles: _Optional[_Union[ApproxQuantilesSpec, _Mapping]] = ..., histogram: _Optional[_Union[HistogramSpec, _Mapping]] = ..., compliance: _Optional[_Union[ComplianceAnalyzerSpec, _Mapping]] = ..., pattern_match: _Optional[_Union[PatternMatchSpec, _Mapping]] = ...) -> None: ...

class DeequAnalysisRelation(_message.Message):
    __slots__ = ("input_relation", "analyzers")
    INPUT_RELATION_FIELD_NUMBER: _ClassVar[int]
    ANALYZERS_FIELD_NUMBER: _ClassVar[int]
    input_relation: bytes
    analyzers: _containers.RepeatedCompositeFieldContainer[Analyzer]
    def __init__(self, input_relation: _Optional[bytes] = ..., analyzers: _Optional[_Iterable[_Union[Analyzer, _Mapping]]] = ...) -> None: ...
