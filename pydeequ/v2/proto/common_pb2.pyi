from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
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
    __slots__ = ("comparison", "range")
    class CompareOp(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        COMPARE_OP_UNSPECIFIED: _ClassVar[Predicate.CompareOp]
        COMPARE_OP_EQ: _ClassVar[Predicate.CompareOp]
        COMPARE_OP_NE: _ClassVar[Predicate.CompareOp]
        COMPARE_OP_GT: _ClassVar[Predicate.CompareOp]
        COMPARE_OP_GE: _ClassVar[Predicate.CompareOp]
        COMPARE_OP_LT: _ClassVar[Predicate.CompareOp]
        COMPARE_OP_LE: _ClassVar[Predicate.CompareOp]
    COMPARE_OP_UNSPECIFIED: Predicate.CompareOp
    COMPARE_OP_EQ: Predicate.CompareOp
    COMPARE_OP_NE: Predicate.CompareOp
    COMPARE_OP_GT: Predicate.CompareOp
    COMPARE_OP_GE: Predicate.CompareOp
    COMPARE_OP_LT: Predicate.CompareOp
    COMPARE_OP_LE: Predicate.CompareOp
    class Comparison(_message.Message):
        __slots__ = ("op", "value")
        OP_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        op: Predicate.CompareOp
        value: float
        def __init__(self, op: _Optional[_Union[Predicate.CompareOp, str]] = ..., value: _Optional[float] = ...) -> None: ...
    class Range(_message.Message):
        __slots__ = ("lower", "upper")
        LOWER_FIELD_NUMBER: _ClassVar[int]
        UPPER_FIELD_NUMBER: _ClassVar[int]
        lower: float
        upper: float
        def __init__(self, lower: _Optional[float] = ..., upper: _Optional[float] = ...) -> None: ...
    COMPARISON_FIELD_NUMBER: _ClassVar[int]
    RANGE_FIELD_NUMBER: _ClassVar[int]
    comparison: Predicate.Comparison
    range: Predicate.Range
    def __init__(self, comparison: _Optional[_Union[Predicate.Comparison, _Mapping]] = ..., range: _Optional[_Union[Predicate.Range, _Mapping]] = ...) -> None: ...

class KLLParameters(_message.Message):
    __slots__ = ("sketch_size", "shrinking_factor", "number_of_buckets")
    SKETCH_SIZE_FIELD_NUMBER: _ClassVar[int]
    SHRINKING_FACTOR_FIELD_NUMBER: _ClassVar[int]
    NUMBER_OF_BUCKETS_FIELD_NUMBER: _ClassVar[int]
    sketch_size: int
    shrinking_factor: float
    number_of_buckets: int
    def __init__(self, sketch_size: _Optional[int] = ..., shrinking_factor: _Optional[float] = ..., number_of_buckets: _Optional[int] = ...) -> None: ...
