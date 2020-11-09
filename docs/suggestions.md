# Constraint Suggestions 

Here are the current supported functionalities of Constraint Suggestions. 

| Class               | Method                                          | Status |
|---------------------|-------------------------------------------------|:------:|
| ConstraintSuggestionRunner      | ConstraintSuggestionRunner(spark_session)                               | Done   |
|  | onData(data)                           | Done   |
| ConstraintSuggestionRunBuilder | ConstraintSuggestionRunBuilder(spark_session, data) | Done |
|  | addConstraintRule(constraintRule) | Done |
|  | run() | Done |
| DEFAULT | DEFAULT() | Done |
| CategoricalRangeRule | CategoricalRangeRule() | Done |
| CompleteIfCompleteRule | CompleteIfCompleteRule() | Done |
| FractionalCategoricalRangeRule | FractionalCategoricalRangeRule(targetDataCoverageFraction) | Done |
| NonNegativeNumbersRule | NonNegativeNumbersRule() | Done |
| RetainCompletenessRule | RetainCompletenessRule() | Done |
| RetainTypeRule | RetainTypeRule | Done |
| UniqueIfApproximatelyUniqueRule | UniqueIfApproximatelyUniqueRule() | Done |

