import common_pb2 as _common_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class DeequColumnProfilerRelation(_message.Message):
    __slots__ = ("input_relation", "restrict_to_columns", "low_cardinality_histogram_threshold", "enable_kll_profiling", "kll_parameters", "predefined_types")
    class PredefinedTypesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    INPUT_RELATION_FIELD_NUMBER: _ClassVar[int]
    RESTRICT_TO_COLUMNS_FIELD_NUMBER: _ClassVar[int]
    LOW_CARDINALITY_HISTOGRAM_THRESHOLD_FIELD_NUMBER: _ClassVar[int]
    ENABLE_KLL_PROFILING_FIELD_NUMBER: _ClassVar[int]
    KLL_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    PREDEFINED_TYPES_FIELD_NUMBER: _ClassVar[int]
    input_relation: bytes
    restrict_to_columns: _containers.RepeatedScalarFieldContainer[str]
    low_cardinality_histogram_threshold: int
    enable_kll_profiling: bool
    kll_parameters: _common_pb2.KLLParameters
    predefined_types: _containers.ScalarMap[str, str]
    def __init__(self, input_relation: _Optional[bytes] = ..., restrict_to_columns: _Optional[_Iterable[str]] = ..., low_cardinality_histogram_threshold: _Optional[int] = ..., enable_kll_profiling: bool = ..., kll_parameters: _Optional[_Union[_common_pb2.KLLParameters, _Mapping]] = ..., predefined_types: _Optional[_Mapping[str, str]] = ...) -> None: ...
