import common_pb2 as _common_pb2
import analysis_pb2 as _analysis_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ColumnSpec(_message.Message):
    __slots__ = ("column",)
    COLUMN_FIELD_NUMBER: _ClassVar[int]
    column: str
    def __init__(self, column: _Optional[str] = ...) -> None: ...

class ColumnAssertionSpec(_message.Message):
    __slots__ = ("column", "assertion")
    COLUMN_FIELD_NUMBER: _ClassVar[int]
    ASSERTION_FIELD_NUMBER: _ClassVar[int]
    column: str
    assertion: _common_pb2.Predicate
    def __init__(self, column: _Optional[str] = ..., assertion: _Optional[_Union[_common_pb2.Predicate, _Mapping]] = ...) -> None: ...

class ColumnsSpec(_message.Message):
    __slots__ = ("columns",)
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    columns: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, columns: _Optional[_Iterable[str]] = ...) -> None: ...

class ColumnsAssertionSpec(_message.Message):
    __slots__ = ("columns", "assertion")
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    ASSERTION_FIELD_NUMBER: _ClassVar[int]
    columns: _containers.RepeatedScalarFieldContainer[str]
    assertion: _common_pb2.Predicate
    def __init__(self, columns: _Optional[_Iterable[str]] = ..., assertion: _Optional[_Union[_common_pb2.Predicate, _Mapping]] = ...) -> None: ...

class SizeSpec(_message.Message):
    __slots__ = ("assertion",)
    ASSERTION_FIELD_NUMBER: _ClassVar[int]
    assertion: _common_pb2.Predicate
    def __init__(self, assertion: _Optional[_Union[_common_pb2.Predicate, _Mapping]] = ...) -> None: ...

class PrimaryKeySpec(_message.Message):
    __slots__ = ("column", "additional_columns")
    COLUMN_FIELD_NUMBER: _ClassVar[int]
    ADDITIONAL_COLUMNS_FIELD_NUMBER: _ClassVar[int]
    column: str
    additional_columns: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, column: _Optional[str] = ..., additional_columns: _Optional[_Iterable[str]] = ...) -> None: ...

class PatternAssertionSpec(_message.Message):
    __slots__ = ("column", "regex", "assertion")
    COLUMN_FIELD_NUMBER: _ClassVar[int]
    REGEX_FIELD_NUMBER: _ClassVar[int]
    ASSERTION_FIELD_NUMBER: _ClassVar[int]
    column: str
    regex: str
    assertion: _common_pb2.Predicate
    def __init__(self, column: _Optional[str] = ..., regex: _Optional[str] = ..., assertion: _Optional[_Union[_common_pb2.Predicate, _Mapping]] = ...) -> None: ...

class PairColumnsAssertionSpec(_message.Message):
    __slots__ = ("column_a", "column_b", "assertion")
    COLUMN_A_FIELD_NUMBER: _ClassVar[int]
    COLUMN_B_FIELD_NUMBER: _ClassVar[int]
    ASSERTION_FIELD_NUMBER: _ClassVar[int]
    column_a: str
    column_b: str
    assertion: _common_pb2.Predicate
    def __init__(self, column_a: _Optional[str] = ..., column_b: _Optional[str] = ..., assertion: _Optional[_Union[_common_pb2.Predicate, _Mapping]] = ...) -> None: ...

class QuantileAssertionSpec(_message.Message):
    __slots__ = ("column", "quantile", "assertion")
    COLUMN_FIELD_NUMBER: _ClassVar[int]
    QUANTILE_FIELD_NUMBER: _ClassVar[int]
    ASSERTION_FIELD_NUMBER: _ClassVar[int]
    column: str
    quantile: float
    assertion: _common_pb2.Predicate
    def __init__(self, column: _Optional[str] = ..., quantile: _Optional[float] = ..., assertion: _Optional[_Union[_common_pb2.Predicate, _Mapping]] = ...) -> None: ...

class AllowedValuesSpec(_message.Message):
    __slots__ = ("column", "allowed_values", "assertion")
    COLUMN_FIELD_NUMBER: _ClassVar[int]
    ALLOWED_VALUES_FIELD_NUMBER: _ClassVar[int]
    ASSERTION_FIELD_NUMBER: _ClassVar[int]
    column: str
    allowed_values: _containers.RepeatedScalarFieldContainer[str]
    assertion: _common_pb2.Predicate
    def __init__(self, column: _Optional[str] = ..., allowed_values: _Optional[_Iterable[str]] = ..., assertion: _Optional[_Union[_common_pb2.Predicate, _Mapping]] = ...) -> None: ...

class SatisfiesSpec(_message.Message):
    __slots__ = ("column_condition", "constraint_name", "assertion")
    COLUMN_CONDITION_FIELD_NUMBER: _ClassVar[int]
    CONSTRAINT_NAME_FIELD_NUMBER: _ClassVar[int]
    ASSERTION_FIELD_NUMBER: _ClassVar[int]
    column_condition: str
    constraint_name: str
    assertion: _common_pb2.Predicate
    def __init__(self, column_condition: _Optional[str] = ..., constraint_name: _Optional[str] = ..., assertion: _Optional[_Union[_common_pb2.Predicate, _Mapping]] = ...) -> None: ...

class Constraint(_message.Message):
    __slots__ = ("hint", "where", "is_complete", "has_completeness", "are_complete", "have_completeness", "has_size", "is_unique", "has_uniqueness", "are_unique", "is_primary_key", "has_distinctness", "has_unique_value_ratio", "has_min", "has_max", "has_mean", "has_sum", "has_standard_deviation", "has_min_length", "has_max_length", "has_approx_quantile", "has_entropy", "has_correlation", "has_pattern", "contains_credit_card_number", "contains_email", "contains_url", "contains_social_security_number", "satisfies", "is_contained_in", "is_non_negative", "is_positive", "is_less_than", "is_less_than_or_equal_to", "is_greater_than", "is_greater_than_or_equal_to", "has_approx_count_distinct")
    HINT_FIELD_NUMBER: _ClassVar[int]
    WHERE_FIELD_NUMBER: _ClassVar[int]
    IS_COMPLETE_FIELD_NUMBER: _ClassVar[int]
    HAS_COMPLETENESS_FIELD_NUMBER: _ClassVar[int]
    ARE_COMPLETE_FIELD_NUMBER: _ClassVar[int]
    HAVE_COMPLETENESS_FIELD_NUMBER: _ClassVar[int]
    HAS_SIZE_FIELD_NUMBER: _ClassVar[int]
    IS_UNIQUE_FIELD_NUMBER: _ClassVar[int]
    HAS_UNIQUENESS_FIELD_NUMBER: _ClassVar[int]
    ARE_UNIQUE_FIELD_NUMBER: _ClassVar[int]
    IS_PRIMARY_KEY_FIELD_NUMBER: _ClassVar[int]
    HAS_DISTINCTNESS_FIELD_NUMBER: _ClassVar[int]
    HAS_UNIQUE_VALUE_RATIO_FIELD_NUMBER: _ClassVar[int]
    HAS_MIN_FIELD_NUMBER: _ClassVar[int]
    HAS_MAX_FIELD_NUMBER: _ClassVar[int]
    HAS_MEAN_FIELD_NUMBER: _ClassVar[int]
    HAS_SUM_FIELD_NUMBER: _ClassVar[int]
    HAS_STANDARD_DEVIATION_FIELD_NUMBER: _ClassVar[int]
    HAS_MIN_LENGTH_FIELD_NUMBER: _ClassVar[int]
    HAS_MAX_LENGTH_FIELD_NUMBER: _ClassVar[int]
    HAS_APPROX_QUANTILE_FIELD_NUMBER: _ClassVar[int]
    HAS_ENTROPY_FIELD_NUMBER: _ClassVar[int]
    HAS_CORRELATION_FIELD_NUMBER: _ClassVar[int]
    HAS_PATTERN_FIELD_NUMBER: _ClassVar[int]
    CONTAINS_CREDIT_CARD_NUMBER_FIELD_NUMBER: _ClassVar[int]
    CONTAINS_EMAIL_FIELD_NUMBER: _ClassVar[int]
    CONTAINS_URL_FIELD_NUMBER: _ClassVar[int]
    CONTAINS_SOCIAL_SECURITY_NUMBER_FIELD_NUMBER: _ClassVar[int]
    SATISFIES_FIELD_NUMBER: _ClassVar[int]
    IS_CONTAINED_IN_FIELD_NUMBER: _ClassVar[int]
    IS_NON_NEGATIVE_FIELD_NUMBER: _ClassVar[int]
    IS_POSITIVE_FIELD_NUMBER: _ClassVar[int]
    IS_LESS_THAN_FIELD_NUMBER: _ClassVar[int]
    IS_LESS_THAN_OR_EQUAL_TO_FIELD_NUMBER: _ClassVar[int]
    IS_GREATER_THAN_FIELD_NUMBER: _ClassVar[int]
    IS_GREATER_THAN_OR_EQUAL_TO_FIELD_NUMBER: _ClassVar[int]
    HAS_APPROX_COUNT_DISTINCT_FIELD_NUMBER: _ClassVar[int]
    hint: str
    where: str
    is_complete: ColumnSpec
    has_completeness: ColumnAssertionSpec
    are_complete: ColumnsSpec
    have_completeness: ColumnsAssertionSpec
    has_size: SizeSpec
    is_unique: ColumnSpec
    has_uniqueness: ColumnsAssertionSpec
    are_unique: ColumnsSpec
    is_primary_key: PrimaryKeySpec
    has_distinctness: ColumnsAssertionSpec
    has_unique_value_ratio: ColumnsAssertionSpec
    has_min: ColumnAssertionSpec
    has_max: ColumnAssertionSpec
    has_mean: ColumnAssertionSpec
    has_sum: ColumnAssertionSpec
    has_standard_deviation: ColumnAssertionSpec
    has_min_length: ColumnAssertionSpec
    has_max_length: ColumnAssertionSpec
    has_approx_quantile: QuantileAssertionSpec
    has_entropy: ColumnAssertionSpec
    has_correlation: PairColumnsAssertionSpec
    has_pattern: PatternAssertionSpec
    contains_credit_card_number: ColumnAssertionSpec
    contains_email: ColumnAssertionSpec
    contains_url: ColumnAssertionSpec
    contains_social_security_number: ColumnAssertionSpec
    satisfies: SatisfiesSpec
    is_contained_in: AllowedValuesSpec
    is_non_negative: ColumnAssertionSpec
    is_positive: ColumnAssertionSpec
    is_less_than: PairColumnsAssertionSpec
    is_less_than_or_equal_to: PairColumnsAssertionSpec
    is_greater_than: PairColumnsAssertionSpec
    is_greater_than_or_equal_to: PairColumnsAssertionSpec
    has_approx_count_distinct: ColumnAssertionSpec
    def __init__(self, hint: _Optional[str] = ..., where: _Optional[str] = ..., is_complete: _Optional[_Union[ColumnSpec, _Mapping]] = ..., has_completeness: _Optional[_Union[ColumnAssertionSpec, _Mapping]] = ..., are_complete: _Optional[_Union[ColumnsSpec, _Mapping]] = ..., have_completeness: _Optional[_Union[ColumnsAssertionSpec, _Mapping]] = ..., has_size: _Optional[_Union[SizeSpec, _Mapping]] = ..., is_unique: _Optional[_Union[ColumnSpec, _Mapping]] = ..., has_uniqueness: _Optional[_Union[ColumnsAssertionSpec, _Mapping]] = ..., are_unique: _Optional[_Union[ColumnsSpec, _Mapping]] = ..., is_primary_key: _Optional[_Union[PrimaryKeySpec, _Mapping]] = ..., has_distinctness: _Optional[_Union[ColumnsAssertionSpec, _Mapping]] = ..., has_unique_value_ratio: _Optional[_Union[ColumnsAssertionSpec, _Mapping]] = ..., has_min: _Optional[_Union[ColumnAssertionSpec, _Mapping]] = ..., has_max: _Optional[_Union[ColumnAssertionSpec, _Mapping]] = ..., has_mean: _Optional[_Union[ColumnAssertionSpec, _Mapping]] = ..., has_sum: _Optional[_Union[ColumnAssertionSpec, _Mapping]] = ..., has_standard_deviation: _Optional[_Union[ColumnAssertionSpec, _Mapping]] = ..., has_min_length: _Optional[_Union[ColumnAssertionSpec, _Mapping]] = ..., has_max_length: _Optional[_Union[ColumnAssertionSpec, _Mapping]] = ..., has_approx_quantile: _Optional[_Union[QuantileAssertionSpec, _Mapping]] = ..., has_entropy: _Optional[_Union[ColumnAssertionSpec, _Mapping]] = ..., has_correlation: _Optional[_Union[PairColumnsAssertionSpec, _Mapping]] = ..., has_pattern: _Optional[_Union[PatternAssertionSpec, _Mapping]] = ..., contains_credit_card_number: _Optional[_Union[ColumnAssertionSpec, _Mapping]] = ..., contains_email: _Optional[_Union[ColumnAssertionSpec, _Mapping]] = ..., contains_url: _Optional[_Union[ColumnAssertionSpec, _Mapping]] = ..., contains_social_security_number: _Optional[_Union[ColumnAssertionSpec, _Mapping]] = ..., satisfies: _Optional[_Union[SatisfiesSpec, _Mapping]] = ..., is_contained_in: _Optional[_Union[AllowedValuesSpec, _Mapping]] = ..., is_non_negative: _Optional[_Union[ColumnAssertionSpec, _Mapping]] = ..., is_positive: _Optional[_Union[ColumnAssertionSpec, _Mapping]] = ..., is_less_than: _Optional[_Union[PairColumnsAssertionSpec, _Mapping]] = ..., is_less_than_or_equal_to: _Optional[_Union[PairColumnsAssertionSpec, _Mapping]] = ..., is_greater_than: _Optional[_Union[PairColumnsAssertionSpec, _Mapping]] = ..., is_greater_than_or_equal_to: _Optional[_Union[PairColumnsAssertionSpec, _Mapping]] = ..., has_approx_count_distinct: _Optional[_Union[ColumnAssertionSpec, _Mapping]] = ...) -> None: ...

class Check(_message.Message):
    __slots__ = ("level", "description", "constraints")
    LEVEL_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    CONSTRAINTS_FIELD_NUMBER: _ClassVar[int]
    level: _common_pb2.CheckLevel
    description: str
    constraints: _containers.RepeatedCompositeFieldContainer[Constraint]
    def __init__(self, level: _Optional[_Union[_common_pb2.CheckLevel, str]] = ..., description: _Optional[str] = ..., constraints: _Optional[_Iterable[_Union[Constraint, _Mapping]]] = ...) -> None: ...

class DeequVerificationRelation(_message.Message):
    __slots__ = ("input_relation", "checks", "required_analyzers")
    INPUT_RELATION_FIELD_NUMBER: _ClassVar[int]
    CHECKS_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_ANALYZERS_FIELD_NUMBER: _ClassVar[int]
    input_relation: bytes
    checks: _containers.RepeatedCompositeFieldContainer[Check]
    required_analyzers: _containers.RepeatedCompositeFieldContainer[_analysis_pb2.Analyzer]
    def __init__(self, input_relation: _Optional[bytes] = ..., checks: _Optional[_Iterable[_Union[Check, _Mapping]]] = ..., required_analyzers: _Optional[_Iterable[_Union[_analysis_pb2.Analyzer, _Mapping]]] = ...) -> None: ...
