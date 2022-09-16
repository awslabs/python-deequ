# -*- coding: utf-8 -*-
"""
Suggestions file for all the Constraint Suggestion classes in Deequ

Author: Calvin Wang
"""
import json

from pyspark.sql import DataFrame, SparkSession

from pydeequ.pandas_utils import ensure_pyspark_df
from pydeequ.configs import IS_DEEQU_V1
from pydeequ.scala_utils import scala_get_default_argument


class ConstraintSuggestionRunner:
    """
    Primary class for interacting with suggestions module

    :param SparkSession spark_session: sparkSession
    """

    def __init__(self, spark_session: SparkSession):
        self._spark_session = spark_session

    def onData(self, df):
        """
        Starting point to construct suggestions

        :param df: Tabular data on which the suggestions module use
        :return: The starting point to construct a suggestions module
        """
        df = ensure_pyspark_df(self._spark_session, df)
        return ConstraintSuggestionRunBuilder(self._spark_session, df)


class ConstraintSuggestionRunBuilder:
    """
    Low level class for running suggestions module

    :param SparkSession spark_session: sparkSession
    :param df: Tabular data on which the suggestions module use
    """

    def __init__(self, spark_session: SparkSession, df: DataFrame):
        self._spark_session = spark_session
        self._jvm = spark_session._jvm
        self._jspark_session = spark_session._jsparkSession
        self._df = df
        self._ConstraintSuggestionRunBuilder = (
            self._jvm.com.amazon.deequ.suggestions.ConstraintSuggestionRunBuilder(df._jdf)
        )

    def addConstraintRule(self, constraintRule):
        """
        Add a single rule for suggesting constraints based on ColumnProfiles to the run.

        :param ConstraintRule constraintRule:  A rule that the dataset will be evaluated on throughout to the run.
                To run all the rules on the dataset use .addConstraintRule(DEFAULT())

        :return  self for further method calls.
        """
        constraintRule._set_jvm(self._jvm)
        constraintRule_jvm = constraintRule.rule_jvm
        if isinstance(constraintRule_jvm, list):
            for rule in constraintRule_jvm:
                rule._set_jvm(self._jvm)
                rule_jvm = rule.rule_jvm
                self._ConstraintSuggestionRunBuilder.addConstraintRule(rule_jvm)

        else:
            self._ConstraintSuggestionRunBuilder.addConstraintRule(constraintRule_jvm)

        return self

    def run(self):
        """
        A method that runs the desired ConstraintSuggestionRunBuilder functions on the data to obtain a constraint
                suggestion result. The result is then translated to python.

        :return: A constraint suggestion result
        """
        result = self._ConstraintSuggestionRunBuilder.run()

        jvmSuggestionResult = self._jvm.com.amazon.deequ.suggestions.ConstraintSuggestionResult
        result_json = json.loads(jvmSuggestionResult.getConstraintSuggestionsAsJson(result))
        for cs in result_json["constraint_suggestions"]:
            cs["code_for_constraint"] = self.__s2p_filter(cs["code_for_constraint"])
        return result_json

    @staticmethod
    def __s2p_filter(code: str):
        """
        Scala -> Python translator for the constraint suggestions code
        A method that returns the python translation of the scala constraint suggestion

        :param str code: a scala constraint suggestion

        :return code that is translated to look more like python code
        """
        if " _ " in code:
            code = code.replace(" _ ", " lambda x: x ")

        if "Some(" in code:
            # Usually at the end as 'where' or 'hint' strings as optional
            code = code.replace("Some(", "")[:-1]

        if "Array(" in code:
            # TODO: what if multiple?
            # TODO: Probz redo with regex
            start = code.index("Array(") + len("Array(")
            for idx in range(start, len(code)):
                if code[idx] == ")":
                    code = code[:idx] + "]" + code[idx + 1 :]
                    code = code.replace("Array(", "[")
                    break

        if "Seq(" in code:
            # TODO: what if multiple?
            # TODO: Probz redo with regex
            start = code.index("Seq(") + len("Seq(")
            for idx in range(start, len(code)):
                if code[idx] == ")":
                    code = code[:idx] + "]" + code[idx + 1 :]
                    code = code.replace("Seq(", "[")
                    break

        return code


class ConstraintSuggestion:

    pass


class ConstraintSuggestionResult:
    """
    The result returned from the ConstraintSuggestionSuite
    """


class _RulesObject:
    """Rules base object to pass and accumulate the rules of the run with respect to the JVM"""

    def _set_jvm(self, jvm):

        self._jvm = jvm
        return self

    @property
    def _deequSuggestions(self):
        if self._jvm:
            return self._jvm.com.amazon.deequ.suggestions
        raise AttributeError(
            "JVM not set, please run _set_jvm() method first."
        )  # TODO: Test that this exception gets raised


class DEFAULT(_RulesObject):
    """
    DEFAULT runs all the rules on the dataset.
    """

    @property
    def rule_jvm(self):
        """

        :return:
        """
        return [
            CategoricalRangeRule(),
            CompleteIfCompleteRule(),
            FractionalCategoricalRangeRule(),
            NonNegativeNumbersRule(),
            RetainCompletenessRule(),
            RetainTypeRule(),
            UniqueIfApproximatelyUniqueRule(),
        ]


class CategoricalRangeRule(_RulesObject):
    """
    If we see a categorical range for a column, we suggest an IS IN (...) constraint
    """

    @property
    def rule_jvm(self):
        if IS_DEEQU_V1:
            return self._deequSuggestions.rules.CategoricalRangeRule()

        # DISCLAIMER: this is a workaround for using the default category sorter
        default_category_sorter = scala_get_default_argument(
            self._deequSuggestions.rules.CategoricalRangeRule, 1
        )
        return self._deequSuggestions.rules.CategoricalRangeRule(default_category_sorter)


class CompleteIfCompleteRule(_RulesObject):
    """
    If a column is complete in the sample, we suggest a NOT NULL constraint
    """

    @property
    def rule_jvm(self):
        return self._deequSuggestions.rules.CompleteIfCompleteRule()


class FractionalCategoricalRangeRule(_RulesObject):
    """
    If we see a categorical range for most values in a column, we suggest an IS IN (...)
    constraint that should hold for most values

    :param float targetDataCoverageFraction: The numerical fraction for which the data should stay within range
    of eachother
    """

    def __init__(self, targetDataCoverageFraction: float = 0.9):

        self.targetDataCoverageFraction = targetDataCoverageFraction

    @property
    def rule_jvm(self):
        if IS_DEEQU_V1:
            return self._deequSuggestions.rules.FractionalCategoricalRangeRule(
                self.targetDataCoverageFraction
            )

        # DISCLAIMER: this is a workaround for using the default category sorter
        default_category_sorter = scala_get_default_argument(
            self._deequSuggestions.rules.FractionalCategoricalRangeRule, 2
        )
        return self._deequSuggestions.rules.FractionalCategoricalRangeRule(
            self.targetDataCoverageFraction, default_category_sorter
        )


class NonNegativeNumbersRule(_RulesObject):
    """
    If we see only non-negative numbers in a column, we suggest a corresponding constraint
    """

    @property
    def rule_jvm(self):
        return self._deequSuggestions.rules.NonNegativeNumbersRule()


class RetainCompletenessRule(_RulesObject):
    """
    If a column is incomplete in the sample, we model its completeness as a binomial variable,
    estimate a confidence interval and use this to define a lower bound for the completeness
    """

    @property
    def rule_jvm(self):
        return self._deequSuggestions.rules.RetainCompletenessRule()


class RetainTypeRule(_RulesObject):
    """
    If we detect a non-string type, we suggest a type constraint
    """

    @property
    def rule_jvm(self):
        return self._deequSuggestions.rules.RetainTypeRule()


class UniqueIfApproximatelyUniqueRule(_RulesObject):
    """
    If the ratio of approximate num distinct values in a column is close to the number of records
    (within error of HLL sketch), we suggest a UNIQUE constraint
    """

    @property
    def rule_jvm(self):
        return self._deequSuggestions.rules.UniqueIfApproximatelyUniqueRule()
