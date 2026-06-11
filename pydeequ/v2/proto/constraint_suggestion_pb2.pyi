import common_pb2 as _common_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class DeequConstraintSuggestionRelation(_message.Message):
    __slots__ = ("input_relation", "constraint_rules", "restrict_to_columns", "low_cardinality_histogram_threshold", "enable_kll_profiling", "kll_parameters", "predefined_types", "testset_ratio", "testset_split_random_seed")
    class PredefinedTypesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    INPUT_RELATION_FIELD_NUMBER: _ClassVar[int]
    CONSTRAINT_RULES_FIELD_NUMBER: _ClassVar[int]
    RESTRICT_TO_COLUMNS_FIELD_NUMBER: _ClassVar[int]
    LOW_CARDINALITY_HISTOGRAM_THRESHOLD_FIELD_NUMBER: _ClassVar[int]
    ENABLE_KLL_PROFILING_FIELD_NUMBER: _ClassVar[int]
    KLL_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    PREDEFINED_TYPES_FIELD_NUMBER: _ClassVar[int]
    TESTSET_RATIO_FIELD_NUMBER: _ClassVar[int]
    TESTSET_SPLIT_RANDOM_SEED_FIELD_NUMBER: _ClassVar[int]
    input_relation: bytes
    constraint_rules: _containers.RepeatedScalarFieldContainer[_common_pb2.ConstraintRuleSet]
    restrict_to_columns: _containers.RepeatedScalarFieldContainer[str]
    low_cardinality_histogram_threshold: int
    enable_kll_profiling: bool
    kll_parameters: _common_pb2.KLLParameters
    predefined_types: _containers.ScalarMap[str, str]
    testset_ratio: float
    testset_split_random_seed: int
    def __init__(self, input_relation: _Optional[bytes] = ..., constraint_rules: _Optional[_Iterable[_Union[_common_pb2.ConstraintRuleSet, str]]] = ..., restrict_to_columns: _Optional[_Iterable[str]] = ..., low_cardinality_histogram_threshold: _Optional[int] = ..., enable_kll_profiling: bool = ..., kll_parameters: _Optional[_Union[_common_pb2.KLLParameters, _Mapping]] = ..., predefined_types: _Optional[_Mapping[str, str]] = ..., testset_ratio: _Optional[float] = ..., testset_split_random_seed: _Optional[int] = ...) -> None: ...
