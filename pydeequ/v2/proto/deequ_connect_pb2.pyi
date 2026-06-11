from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CheckLevel(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CHECK_LEVEL_UNSPECIFIED: _ClassVar[CheckLevel]
    CHECK_LEVEL_ERROR: _ClassVar[CheckLevel]
    CHECK_LEVEL_WARNING: _ClassVar[CheckLevel]

class ConstraintRuleSet(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CONSTRAINT_RULE_SET_UNSPECIFIED: _ClassVar[ConstraintRuleSet]
    CONSTRAINT_RULE_SET_DEFAULT: _ClassVar[ConstraintRuleSet]
    CONSTRAINT_RULE_SET_STRING: _ClassVar[ConstraintRuleSet]
    CONSTRAINT_RULE_SET_NUMERICAL: _ClassVar[ConstraintRuleSet]
    CONSTRAINT_RULE_SET_COMMON: _ClassVar[ConstraintRuleSet]
    CONSTRAINT_RULE_SET_EXTENDED: _ClassVar[ConstraintRuleSet]
CHECK_LEVEL_UNSPECIFIED: CheckLevel
CHECK_LEVEL_ERROR: CheckLevel
CHECK_LEVEL_WARNING: CheckLevel
CONSTRAINT_RULE_SET_UNSPECIFIED: ConstraintRuleSet
CONSTRAINT_RULE_SET_DEFAULT: ConstraintRuleSet
CONSTRAINT_RULE_SET_STRING: ConstraintRuleSet
CONSTRAINT_RULE_SET_NUMERICAL: ConstraintRuleSet
CONSTRAINT_RULE_SET_COMMON: ConstraintRuleSet
CONSTRAINT_RULE_SET_EXTENDED: ConstraintRuleSet

class Predicate(_message.Message):
    __slots__ = ("op", "value", "lower_bound", "upper_bound")
    class CompareOp(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        COMPARE_OP_UNSPECIFIED: _ClassVar[Predicate.CompareOp]
        COMPARE_OP_EQ: _ClassVar[Predicate.CompareOp]
        COMPARE_OP_NE: _ClassVar[Predicate.CompareOp]
        COMPARE_OP_GT: _ClassVar[Predicate.CompareOp]
        COMPARE_OP_GE: _ClassVar[Predicate.CompareOp]
        COMPARE_OP_LT: _ClassVar[Predicate.CompareOp]
        COMPARE_OP_LE: _ClassVar[Predicate.CompareOp]
        COMPARE_OP_BETWEEN: _ClassVar[Predicate.CompareOp]
    COMPARE_OP_UNSPECIFIED: Predicate.CompareOp
    COMPARE_OP_EQ: Predicate.CompareOp
    COMPARE_OP_NE: Predicate.CompareOp
    COMPARE_OP_GT: Predicate.CompareOp
    COMPARE_OP_GE: Predicate.CompareOp
    COMPARE_OP_LT: Predicate.CompareOp
    COMPARE_OP_LE: Predicate.CompareOp
    COMPARE_OP_BETWEEN: Predicate.CompareOp
    OP_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    LOWER_BOUND_FIELD_NUMBER: _ClassVar[int]
    UPPER_BOUND_FIELD_NUMBER: _ClassVar[int]
    op: Predicate.CompareOp
    value: float
    lower_bound: float
    upper_bound: float
    def __init__(self, op: _Optional[_Union[Predicate.CompareOp, str]] = ..., value: _Optional[float] = ..., lower_bound: _Optional[float] = ..., upper_bound: _Optional[float] = ...) -> None: ...

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
    assertion: Predicate
    def __init__(self, column: _Optional[str] = ..., assertion: _Optional[_Union[Predicate, _Mapping]] = ...) -> None: ...

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
    assertion: Predicate
    def __init__(self, columns: _Optional[_Iterable[str]] = ..., assertion: _Optional[_Union[Predicate, _Mapping]] = ...) -> None: ...

class SizeSpec(_message.Message):
    __slots__ = ("assertion",)
    ASSERTION_FIELD_NUMBER: _ClassVar[int]
    assertion: Predicate
    def __init__(self, assertion: _Optional[_Union[Predicate, _Mapping]] = ...) -> None: ...

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
    assertion: Predicate
    def __init__(self, column: _Optional[str] = ..., regex: _Optional[str] = ..., assertion: _Optional[_Union[Predicate, _Mapping]] = ...) -> None: ...

class PairColumnsAssertionSpec(_message.Message):
    __slots__ = ("column_a", "column_b", "assertion")
    COLUMN_A_FIELD_NUMBER: _ClassVar[int]
    COLUMN_B_FIELD_NUMBER: _ClassVar[int]
    ASSERTION_FIELD_NUMBER: _ClassVar[int]
    column_a: str
    column_b: str
    assertion: Predicate
    def __init__(self, column_a: _Optional[str] = ..., column_b: _Optional[str] = ..., assertion: _Optional[_Union[Predicate, _Mapping]] = ...) -> None: ...

class QuantileAssertionSpec(_message.Message):
    __slots__ = ("column", "quantile", "assertion")
    COLUMN_FIELD_NUMBER: _ClassVar[int]
    QUANTILE_FIELD_NUMBER: _ClassVar[int]
    ASSERTION_FIELD_NUMBER: _ClassVar[int]
    column: str
    quantile: float
    assertion: Predicate
    def __init__(self, column: _Optional[str] = ..., quantile: _Optional[float] = ..., assertion: _Optional[_Union[Predicate, _Mapping]] = ...) -> None: ...

class AllowedValuesSpec(_message.Message):
    __slots__ = ("column", "allowed_values", "assertion")
    COLUMN_FIELD_NUMBER: _ClassVar[int]
    ALLOWED_VALUES_FIELD_NUMBER: _ClassVar[int]
    ASSERTION_FIELD_NUMBER: _ClassVar[int]
    column: str
    allowed_values: _containers.RepeatedScalarFieldContainer[str]
    assertion: Predicate
    def __init__(self, column: _Optional[str] = ..., allowed_values: _Optional[_Iterable[str]] = ..., assertion: _Optional[_Union[Predicate, _Mapping]] = ...) -> None: ...

class SatisfiesSpec(_message.Message):
    __slots__ = ("column_condition", "constraint_name", "assertion")
    COLUMN_CONDITION_FIELD_NUMBER: _ClassVar[int]
    CONSTRAINT_NAME_FIELD_NUMBER: _ClassVar[int]
    ASSERTION_FIELD_NUMBER: _ClassVar[int]
    column_condition: str
    constraint_name: str
    assertion: Predicate
    def __init__(self, column_condition: _Optional[str] = ..., constraint_name: _Optional[str] = ..., assertion: _Optional[_Union[Predicate, _Mapping]] = ...) -> None: ...

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
    level: CheckLevel
    description: str
    constraints: _containers.RepeatedCompositeFieldContainer[Constraint]
    def __init__(self, level: _Optional[_Union[CheckLevel, str]] = ..., description: _Optional[str] = ..., constraints: _Optional[_Iterable[_Union[Constraint, _Mapping]]] = ...) -> None: ...

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

class KLLParameters(_message.Message):
    __slots__ = ("sketch_size", "shrinking_factor", "number_of_buckets")
    SKETCH_SIZE_FIELD_NUMBER: _ClassVar[int]
    SHRINKING_FACTOR_FIELD_NUMBER: _ClassVar[int]
    NUMBER_OF_BUCKETS_FIELD_NUMBER: _ClassVar[int]
    sketch_size: int
    shrinking_factor: float
    number_of_buckets: int
    def __init__(self, sketch_size: _Optional[int] = ..., shrinking_factor: _Optional[float] = ..., number_of_buckets: _Optional[int] = ...) -> None: ...

class DeequVerificationRelation(_message.Message):
    __slots__ = ("input_relation", "checks", "required_analyzers")
    INPUT_RELATION_FIELD_NUMBER: _ClassVar[int]
    CHECKS_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_ANALYZERS_FIELD_NUMBER: _ClassVar[int]
    input_relation: bytes
    checks: _containers.RepeatedCompositeFieldContainer[Check]
    required_analyzers: _containers.RepeatedCompositeFieldContainer[Analyzer]
    def __init__(self, input_relation: _Optional[bytes] = ..., checks: _Optional[_Iterable[_Union[Check, _Mapping]]] = ..., required_analyzers: _Optional[_Iterable[_Union[Analyzer, _Mapping]]] = ...) -> None: ...

class DeequAnalysisRelation(_message.Message):
    __slots__ = ("input_relation", "analyzers")
    INPUT_RELATION_FIELD_NUMBER: _ClassVar[int]
    ANALYZERS_FIELD_NUMBER: _ClassVar[int]
    input_relation: bytes
    analyzers: _containers.RepeatedCompositeFieldContainer[Analyzer]
    def __init__(self, input_relation: _Optional[bytes] = ..., analyzers: _Optional[_Iterable[_Union[Analyzer, _Mapping]]] = ...) -> None: ...

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
    kll_parameters: KLLParameters
    predefined_types: _containers.ScalarMap[str, str]
    def __init__(self, input_relation: _Optional[bytes] = ..., restrict_to_columns: _Optional[_Iterable[str]] = ..., low_cardinality_histogram_threshold: _Optional[int] = ..., enable_kll_profiling: bool = ..., kll_parameters: _Optional[_Union[KLLParameters, _Mapping]] = ..., predefined_types: _Optional[_Mapping[str, str]] = ...) -> None: ...

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
    constraint_rules: _containers.RepeatedScalarFieldContainer[ConstraintRuleSet]
    restrict_to_columns: _containers.RepeatedScalarFieldContainer[str]
    low_cardinality_histogram_threshold: int
    enable_kll_profiling: bool
    kll_parameters: KLLParameters
    predefined_types: _containers.ScalarMap[str, str]
    testset_ratio: float
    testset_split_random_seed: int
    def __init__(self, input_relation: _Optional[bytes] = ..., constraint_rules: _Optional[_Iterable[_Union[ConstraintRuleSet, str]]] = ..., restrict_to_columns: _Optional[_Iterable[str]] = ..., low_cardinality_histogram_threshold: _Optional[int] = ..., enable_kll_profiling: bool = ..., kll_parameters: _Optional[_Union[KLLParameters, _Mapping]] = ..., predefined_types: _Optional[_Mapping[str, str]] = ..., testset_ratio: _Optional[float] = ..., testset_split_random_seed: _Optional[int] = ...) -> None: ...
