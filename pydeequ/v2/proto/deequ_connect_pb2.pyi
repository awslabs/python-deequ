from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class VerificationStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    VERIFICATION_SUCCESS: _ClassVar[VerificationStatus]
    VERIFICATION_WARNING: _ClassVar[VerificationStatus]
    VERIFICATION_ERROR: _ClassVar[VerificationStatus]

class CheckStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CHECK_SUCCESS: _ClassVar[CheckStatus]
    CHECK_WARNING: _ClassVar[CheckStatus]
    CHECK_ERROR: _ClassVar[CheckStatus]

class ConstraintStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CONSTRAINT_SUCCESS: _ClassVar[ConstraintStatus]
    CONSTRAINT_FAILURE: _ClassVar[ConstraintStatus]

class MetricEntity(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DATASET: _ClassVar[MetricEntity]
    COLUMN: _ClassVar[MetricEntity]
    MULTICOLUMN: _ClassVar[MetricEntity]
VERIFICATION_SUCCESS: VerificationStatus
VERIFICATION_WARNING: VerificationStatus
VERIFICATION_ERROR: VerificationStatus
CHECK_SUCCESS: CheckStatus
CHECK_WARNING: CheckStatus
CHECK_ERROR: CheckStatus
CONSTRAINT_SUCCESS: ConstraintStatus
CONSTRAINT_FAILURE: ConstraintStatus
DATASET: MetricEntity
COLUMN: MetricEntity
MULTICOLUMN: MetricEntity

class DeequVerificationRelation(_message.Message):
    __slots__ = ()
    INPUT_RELATION_FIELD_NUMBER: _ClassVar[int]
    CHECKS_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_ANALYZERS_FIELD_NUMBER: _ClassVar[int]
    input_relation: bytes
    checks: _containers.RepeatedCompositeFieldContainer[CheckMessage]
    required_analyzers: _containers.RepeatedCompositeFieldContainer[AnalyzerMessage]
    def __init__(self, input_relation: _Optional[bytes] = ..., checks: _Optional[_Iterable[_Union[CheckMessage, _Mapping]]] = ..., required_analyzers: _Optional[_Iterable[_Union[AnalyzerMessage, _Mapping]]] = ...) -> None: ...

class DeequAnalysisRelation(_message.Message):
    __slots__ = ()
    INPUT_RELATION_FIELD_NUMBER: _ClassVar[int]
    ANALYZERS_FIELD_NUMBER: _ClassVar[int]
    input_relation: bytes
    analyzers: _containers.RepeatedCompositeFieldContainer[AnalyzerMessage]
    def __init__(self, input_relation: _Optional[bytes] = ..., analyzers: _Optional[_Iterable[_Union[AnalyzerMessage, _Mapping]]] = ...) -> None: ...

class CheckMessage(_message.Message):
    __slots__ = ()
    class Level(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        ERROR: _ClassVar[CheckMessage.Level]
        WARNING: _ClassVar[CheckMessage.Level]
    ERROR: CheckMessage.Level
    WARNING: CheckMessage.Level
    LEVEL_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    CONSTRAINTS_FIELD_NUMBER: _ClassVar[int]
    level: CheckMessage.Level
    description: str
    constraints: _containers.RepeatedCompositeFieldContainer[ConstraintMessage]
    def __init__(self, level: _Optional[_Union[CheckMessage.Level, str]] = ..., description: _Optional[str] = ..., constraints: _Optional[_Iterable[_Union[ConstraintMessage, _Mapping]]] = ...) -> None: ...

class ConstraintMessage(_message.Message):
    __slots__ = ()
    TYPE_FIELD_NUMBER: _ClassVar[int]
    COLUMN_FIELD_NUMBER: _ClassVar[int]
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    ASSERTION_FIELD_NUMBER: _ClassVar[int]
    HINT_FIELD_NUMBER: _ClassVar[int]
    WHERE_FIELD_NUMBER: _ClassVar[int]
    PATTERN_FIELD_NUMBER: _ClassVar[int]
    COLUMN_CONDITION_FIELD_NUMBER: _ClassVar[int]
    CONSTRAINT_NAME_FIELD_NUMBER: _ClassVar[int]
    ALLOWED_VALUES_FIELD_NUMBER: _ClassVar[int]
    QUANTILE_FIELD_NUMBER: _ClassVar[int]
    type: str
    column: str
    columns: _containers.RepeatedScalarFieldContainer[str]
    assertion: PredicateMessage
    hint: str
    where: str
    pattern: str
    column_condition: str
    constraint_name: str
    allowed_values: _containers.RepeatedScalarFieldContainer[str]
    quantile: float
    def __init__(self, type: _Optional[str] = ..., column: _Optional[str] = ..., columns: _Optional[_Iterable[str]] = ..., assertion: _Optional[_Union[PredicateMessage, _Mapping]] = ..., hint: _Optional[str] = ..., where: _Optional[str] = ..., pattern: _Optional[str] = ..., column_condition: _Optional[str] = ..., constraint_name: _Optional[str] = ..., allowed_values: _Optional[_Iterable[str]] = ..., quantile: _Optional[float] = ...) -> None: ...

class PredicateMessage(_message.Message):
    __slots__ = ()
    class Operator(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        UNSPECIFIED: _ClassVar[PredicateMessage.Operator]
        EQ: _ClassVar[PredicateMessage.Operator]
        NE: _ClassVar[PredicateMessage.Operator]
        GT: _ClassVar[PredicateMessage.Operator]
        GE: _ClassVar[PredicateMessage.Operator]
        LT: _ClassVar[PredicateMessage.Operator]
        LE: _ClassVar[PredicateMessage.Operator]
        BETWEEN: _ClassVar[PredicateMessage.Operator]
    UNSPECIFIED: PredicateMessage.Operator
    EQ: PredicateMessage.Operator
    NE: PredicateMessage.Operator
    GT: PredicateMessage.Operator
    GE: PredicateMessage.Operator
    LT: PredicateMessage.Operator
    LE: PredicateMessage.Operator
    BETWEEN: PredicateMessage.Operator
    OPERATOR_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    LOWER_BOUND_FIELD_NUMBER: _ClassVar[int]
    UPPER_BOUND_FIELD_NUMBER: _ClassVar[int]
    operator: PredicateMessage.Operator
    value: float
    lower_bound: float
    upper_bound: float
    def __init__(self, operator: _Optional[_Union[PredicateMessage.Operator, str]] = ..., value: _Optional[float] = ..., lower_bound: _Optional[float] = ..., upper_bound: _Optional[float] = ...) -> None: ...

class AnalyzerMessage(_message.Message):
    __slots__ = ()
    TYPE_FIELD_NUMBER: _ClassVar[int]
    COLUMN_FIELD_NUMBER: _ClassVar[int]
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    WHERE_FIELD_NUMBER: _ClassVar[int]
    QUANTILE_FIELD_NUMBER: _ClassVar[int]
    RELATIVE_ERROR_FIELD_NUMBER: _ClassVar[int]
    PATTERN_FIELD_NUMBER: _ClassVar[int]
    MAX_DETAIL_BINS_FIELD_NUMBER: _ClassVar[int]
    KLL_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    type: str
    column: str
    columns: _containers.RepeatedScalarFieldContainer[str]
    where: str
    quantile: float
    relative_error: float
    pattern: str
    max_detail_bins: int
    kll_parameters: KLLParameters
    def __init__(self, type: _Optional[str] = ..., column: _Optional[str] = ..., columns: _Optional[_Iterable[str]] = ..., where: _Optional[str] = ..., quantile: _Optional[float] = ..., relative_error: _Optional[float] = ..., pattern: _Optional[str] = ..., max_detail_bins: _Optional[int] = ..., kll_parameters: _Optional[_Union[KLLParameters, _Mapping]] = ...) -> None: ...

class KLLParameters(_message.Message):
    __slots__ = ()
    SKETCH_SIZE_FIELD_NUMBER: _ClassVar[int]
    SHRINKING_FACTOR_FIELD_NUMBER: _ClassVar[int]
    NUMBER_OF_BUCKETS_FIELD_NUMBER: _ClassVar[int]
    sketch_size: int
    shrinking_factor: float
    number_of_buckets: int
    def __init__(self, sketch_size: _Optional[int] = ..., shrinking_factor: _Optional[float] = ..., number_of_buckets: _Optional[int] = ...) -> None: ...

class DeequColumnProfilerRelation(_message.Message):
    __slots__ = ()
    class PredefinedTypesEntry(_message.Message):
        __slots__ = ()
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
    kll_parameters: KLLParameters
    predefined_types: _containers.ScalarMap[str, str]
    def __init__(self, input_relation: _Optional[bytes] = ..., restrict_to_columns: _Optional[_Iterable[str]] = ..., low_cardinality_histogram_threshold: _Optional[int] = ..., enable_kll_profiling: _Optional[bool] = ..., kll_parameters: _Optional[_Union[KLLParameters, _Mapping]] = ..., predefined_types: _Optional[_Mapping[str, str]] = ...) -> None: ...

class DeequConstraintSuggestionRelation(_message.Message):
    __slots__ = ()
    class PredefinedTypesEntry(_message.Message):
        __slots__ = ()
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
    constraint_rules: _containers.RepeatedScalarFieldContainer[str]
    restrict_to_columns: _containers.RepeatedScalarFieldContainer[str]
    low_cardinality_histogram_threshold: int
    enable_kll_profiling: bool
    kll_parameters: KLLParameters
    predefined_types: _containers.ScalarMap[str, str]
    testset_ratio: float
    testset_split_random_seed: int
    def __init__(self, input_relation: _Optional[bytes] = ..., constraint_rules: _Optional[_Iterable[str]] = ..., restrict_to_columns: _Optional[_Iterable[str]] = ..., low_cardinality_histogram_threshold: _Optional[int] = ..., enable_kll_profiling: _Optional[bool] = ..., kll_parameters: _Optional[_Union[KLLParameters, _Mapping]] = ..., predefined_types: _Optional[_Mapping[str, str]] = ..., testset_ratio: _Optional[float] = ..., testset_split_random_seed: _Optional[int] = ...) -> None: ...
