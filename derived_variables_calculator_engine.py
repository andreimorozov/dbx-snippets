# Databricks notebook source
# MAGIC %md # Derived Variables Calculator Engine

# COMMAND ----------

# MAGIC %md ## Overview
# MAGIC This notebook contains the internals for the Derived Variables Calculator, such as:
# MAGIC - abstract Calculator
# MAGIC - concrete Calculator implemetations
# MAGIC - Calculator factory
# MAGIC - enums

# COMMAND ----------

# DBTITLE 1,Definitions for Calculator, enums
from enum import Enum
import numpy as np
import ast

class PostCalculationInstruction(Enum):
    MOVE_TO_NEXT_VAR__VALUE_RESOLVED = 1
    MOVE_TO_NEXT_RULE__KEYS_EXIST_CONDITIONS_NOT_MET = 2
    MOVE_TO_NEXT_VAR__UNDERLYING_DATA_NOT_FOUND = 3
    MOVE_TO_NEXT_VAR__WILL_ATTEMPT_TO_CALCULATE_ON_THE_NEXT_PASS = 4
    STOP__ALL_DONE = 5
    
from abc import ABC, abstractmethod
from pandas import Series


class Calculator(ABC):
    def __init__(self, row_variable_lookup: Series):
        self._row_variable_lookup = row_variable_lookup
        self._is_printing_output_messages = True
        self._divider = "---------------------------------------------------------------------------------------------------------------------------------------------------------------------"

    # ===============================================================================
    # PROPERTIES
    # ===============================================================================

    @property
    def is_printing_output_messages(self):
        return self._is_printing_output_messages

    @is_printing_output_messages.setter
    def is_printing_output_messages(self, value):
        self._is_printing_output_messages = value

    @property
    def row_variable_lookup(self):
        return self._row_variable_lookup

    @property
    def key_a(self):
        return self._row_variable_lookup["survey_id_a"]

    @property
    def value_a(self):
        return self._row_variable_lookup["survey_id_a_value_1"]

    @property
    def value_a2(self):
        return self._row_variable_lookup["survey_id_a_value_2"]

    @property
    def key_b(self):
        return self._row_variable_lookup["survey_id_b"]

    @property
    def value_b(self):
        return self._row_variable_lookup["survey_id_b_value"]

    @property
    def key_c(self):
        return self._row_variable_lookup["survey_id_c"]

    @property
    def value_c(self):
        return self._row_variable_lookup["survey_id_c_value"]

    @property
    def key_d(self):
        return self._row_variable_lookup["survey_id_d"]

    @property
    def value_d(self):
        return self._row_variable_lookup["survey_id_d_value"]

    @property
    def action(self):
        return self._row_variable_lookup["action"]

    @property
    def detail(self):
        return self._row_variable_lookup["detail"]

    @property
    def new_var_name(self):
        return self._row_variable_lookup["new_variable"]

    @property
    def new_var_value(self):
        return self._row_variable_lookup["fill_with_this"]

    @property
    def else_value(self):
        return self._row_variable_lookup["else"]

    # ===============================================================================
    # Utility methods
    # ===============================================================================

    def actual_value_in_response_a(self, row_response: tuple):
        return row_response["values"][self.key_a]

    def handle_new_var_value(self) -> (PostCalculationInstruction, str):
        self.print_output_message(f"Resolved to {self.new_var_value}")
        return PostCalculationInstruction.MOVE_TO_NEXT_VAR__VALUE_RESOLVED, self.new_var_value

    def handle_else_value(self) -> (PostCalculationInstruction, str):        
        # if self.else_value is not None:
        # isfloat(actual_value_in_the_response)
#         if self.else_value is not None and not np.isnan(self.else_value):
        if self.else_value is not None and not self.isfloat(self.else_value):
            # I've never seen else value being provided.
            self.print_output_message(f"Resolved to {self.else_value}")
            return PostCalculationInstruction.MOVE_TO_NEXT_VAR__VALUE_RESOLVED, self.else_value
        else:
            self.print_output_message(f"condition not met")
            return PostCalculationInstruction.MOVE_TO_NEXT_RULE__KEYS_EXIST_CONDITIONS_NOT_MET, ""

    def print_top(self):
        self.print_output_message(self._divider)
        self.print_output_message(f"Derived variable name: {self.new_var_name}")
        self.print_output_message(f"Evaluator: {type(self).__name__}")
        self.print_output_message(f"action: {self.action}; detail: {self.detail}")

    def print_bottom(self):
        self.print_output_message(self._divider)

    def print_key_not_found_error(self, e: KeyError):
        self.print_output_message(f"KeyError exception in {(type(self)).__name__}: Key {e} does not exist.")
        self.print_output_message(self._divider)

    def print_output_message(self, message: str):
        if self.is_printing_output_messages:
            print(message)
    
    @staticmethod
    def isfloat(value):
        try:
          float(value)
          return True
        except ValueError:
          return False
    
    @staticmethod
    def convert_str_to_list(value):
        if isinstance(value, list):
            new_list = []
            for x in value:
                new_list.append(float(x))
            return new_list
        else:
            if "[" not in str(value):
                value = "[" + str(value) + "]"
            return ast.literal_eval(value)
      
    @staticmethod
    def check_key(d, key):      
        if key in d.keys():
            return True
        else:
            return False

    # ===============================================================================
    # MAIN METHODS
    # ===============================================================================

    def produce_new_var(self, row_response: tuple) -> (PostCalculationInstruction, str):
        """
                Makes an attempt to output a value for a new variable if the source data exists (keys found) and conditions met
                If values is resolved successfully, then PostCalculationInstruction = MOVE_TO_NEXT_VAR_VALUE_RESOLVED is returned, the calculated value is included in the response
                If source data exists (keys found), but conditions not satisfied, then PostCalculationInstruction = MOVE_TO_NEXT_RULE__KEYS_EXIST_CONDITIONS_NOT_MET is returned
                If source data doesn't exist (keys not found at the iteration where they are expected), then PostCalculationInstruction = MOVE_TO_NEXT_VAR__UNDERLYING_DATA_NOT_FOUND is returned
                If source data is not produced yet and we have to wait until next pass, then PostCalculationInstruction = MOVE_TO_NEXT_VAR__WILL_ATTEMPT_TO_CALCULATE_ON_THE_NEXT_PASS is returned
                If all checked are finished, then PostCalculationInstruction = STOP__ALL_DONE is returned

                :rtype: (PostCalculationInstruction, str)
                """
        try:
            self.print_top()
            result = self.evaluate(row_response)
            self.print_bottom()
            return result
        except KeyError as e:
            self.print_key_not_found_error(e)
            return PostCalculationInstruction.MOVE_TO_NEXT_VAR__UNDERLYING_DATA_NOT_FOUND, ""

    @abstractmethod
    def evaluate(self, row_response: tuple) -> (PostCalculationInstruction, str):
        raise NotImplementedError

# COMMAND ----------

# DBTITLE 1,Concrete Calculators
import statistics

class Calculator__Conditional_Equal(Calculator):
    def __init__(self, row_variable_lookup: tuple):
        super().__init__(row_variable_lookup)

    def evaluate(self, row_response: tuple) -> (PostCalculationInstruction, str):
        actual_value_in_the_response = str(super().actual_value_in_response_a(row_response))
        value_to_compare_with = str(super().value_a)
        super().print_output_message(
            f"Calculator__Conditional_Equal; key_to_find: {super().key_a}; "
            f"formula: if actual_value_in_the_response == value_to_compare_with then new_var_value else else_value; "
            f"if {actual_value_in_the_response} == {value_to_compare_with} then {super().new_var_value} else {super().else_value}")
        if super().isfloat(actual_value_in_the_response) and super().isfloat(value_to_compare_with):
            if float(actual_value_in_the_response) == float(value_to_compare_with):
                return super().handle_new_var_value()
            else:
                return super().handle_else_value()
        return PostCalculationInstruction.MOVE_TO_NEXT_VAR__UNDERLYING_DATA_NOT_FOUND, ""


class Calculator__Conditional_EqualString(Calculator):
    def __init__(self, row_variable_lookup: tuple):
        super().__init__(row_variable_lookup)

    def evaluate(self, row_response: tuple) -> (PostCalculationInstruction, str):
        actual_value_in_the_response = super().actual_value_in_response_a(row_response)
        value_to_compare_with = super().value_a
        super().print_output_message(
            f"Calculator__Conditional_EqualString; key_to_find: {super().key_a}; "
            f"formula: if actual_value_in_the_response == value_to_compare_with then new_var_value else else_value; "
            f"if {actual_value_in_the_response} == {value_to_compare_with} then {super().new_var_value} else {super().else_value}")

        if actual_value_in_the_response == value_to_compare_with:
            return super().handle_new_var_value()
        else:
            return super().handle_else_value()


class Calculator__Conditional_GreaterThan(Calculator):
    def __init__(self, row_variable_lookup: tuple):
        super().__init__(row_variable_lookup)

    def evaluate(self, row_response: tuple) -> (PostCalculationInstruction, str):
        actual_value_in_the_response = str(super().actual_value_in_response_a(row_response))
        value_to_compare_with = str(super().value_a)
        super().print_output_message(f"Calculator__Conditional_GreaterThan; key_to_find: {super().key_a}; "
                                     f"formula: if actual_value_in_the_response > value_to_compare_with then new_var_value else else_value; "
                                     f"if {actual_value_in_the_response} > {value_to_compare_with} then {super().new_var_value} else {super().else_value}")
        if super().isfloat(actual_value_in_the_response) and super().isfloat(value_to_compare_with):
            if float(actual_value_in_the_response) > float(value_to_compare_with):
                return super().handle_new_var_value()
            else:
                return super().handle_else_value()
        return PostCalculationInstruction.MOVE_TO_NEXT_VAR__UNDERLYING_DATA_NOT_FOUND, ""


class Calculator_Conditional_GreaterThanEqual(Calculator):
    def __init__(self, row_variable_lookup: tuple):
        super().__init__(row_variable_lookup)

    def evaluate(self, row_response: tuple) -> (PostCalculationInstruction, str):
        actual_value_in_the_response = str(super().actual_value_in_response_a(row_response))
        value_to_compare_with = str(super().value_a)
        super().print_output_message(
            f"Calculator_Conditional_GreaterThanEqual; key_to_find: {super().key_a}; "
            f"formula: if actual_value_in_the_response >= value_to_compare_with then new_var_value else else_value; "
            f"if {actual_value_in_the_response} >= {value_to_compare_with} then {super().new_var_value} else {super().else_value}")
        if super().isfloat(actual_value_in_the_response) and super().isfloat(value_to_compare_with):
            if float(actual_value_in_the_response) >= float(value_to_compare_with):
                return super().handle_new_var_value()
            else:
                return super().handle_else_value()
        return PostCalculationInstruction.MOVE_TO_NEXT_VAR__UNDERLYING_DATA_NOT_FOUND, ""


class Calculator__Conditional_IsIn(Calculator):
    def __init__(self, row_variable_lookup: tuple):
        super().__init__(row_variable_lookup)

    def evaluate(self, row_response: tuple) -> (PostCalculationInstruction, str):
        actual_value_in_the_response = str(super().actual_value_in_response_a(row_response))
        value_to_compare_with = str(super().value_a)
        list_to_use_for_comparison = value_to_compare_with.split(",")
        super().print_output_message(
            f"Calculator__Conditional_IsIn; key_to_find: {super().key_a}; "
            f"formula: if actual_value_in_the_response is in the values provided by comma separated value_to_compare_with then new_var_value else else_value; "
            f"actual value: {actual_value_in_the_response}; Value to compare with: {value_to_compare_with} then {super().new_var_value} else {super().else_value}")

        if actual_value_in_the_response in list_to_use_for_comparison:
            return super().handle_new_var_value()
        else:
            return super().handle_else_value()


class Calculator_Conditional_LessThan(Calculator):
    def __init__(self, row_variable_lookup: tuple):
        super().__init__(row_variable_lookup)

    def evaluate(self, row_response: tuple) -> (PostCalculationInstruction, str):
        actual_value_in_the_response = str(super().actual_value_in_response_a(row_response))
        value_to_compare_with = str(super().value_a)
        super().print_output_message(f"Calculator_Conditional_LessThan; key_to_find: {super().key_a}; "
                                     f"formula: if actual_value_in_the_response < value_to_compare_with then new_var_value else else_value; "
                                     f"if {actual_value_in_the_response} < {value_to_compare_with} then {super().new_var_value} else {super().else_value}")
        if super().isfloat(actual_value_in_the_response) and super().isfloat(value_to_compare_with):
            if float(actual_value_in_the_response) < float(value_to_compare_with):
                return super().handle_new_var_value()
            else:
                return super().handle_else_value()
        return PostCalculationInstruction.MOVE_TO_NEXT_VAR__UNDERLYING_DATA_NOT_FOUND, ""


class Calculator_Conditional_LessThanEqual(Calculator):
    def __init__(self, row_variable_lookup: tuple):
        super().__init__(row_variable_lookup)

    def evaluate(self, row_response: tuple) -> (PostCalculationInstruction, str):
        actual_value_in_the_response = str(super().actual_value_in_response_a(row_response))
        value_to_compare_with = str(super().value_a)
        super().print_output_message(f"Calculator_Conditional_LessThanEqual; key_to_find: {super().key_a}; formula: "
                                     f"if actual_value_in_the_response <= value_to_compare_with then new_var_value else else_value; "
                                     f"if {actual_value_in_the_response} <= {value_to_compare_with} then {super().new_var_value} else {super().else_value}")
        if super().isfloat(actual_value_in_the_response) and super().isfloat(value_to_compare_with):
            if float(actual_value_in_the_response) <= float(value_to_compare_with):
                return super().handle_new_var_value()
            else:
                return super().handle_else_value()
        return PostCalculationInstruction.MOVE_TO_NEXT_VAR__UNDERLYING_DATA_NOT_FOUND, ""


class Calculator_Mean(Calculator):
    def __init__(self, row_variable_lookup: tuple):
        super().__init__(row_variable_lookup)

    def evaluate(self, row_response: tuple) -> (PostCalculationInstruction, str):
        super().print_output_message("Calculator_Mean: find mean from values mapped to keys in comma-separated list coming from survey_id_a")
        super().print_output_message(f"Here are the keys: {super().key_a}")
        list_of_keys_to_use_for_sum = super().key_a.split(",")
        values_to_mean = []
        for s in list_of_keys_to_use_for_sum:
            values_to_mean.append(float(row_response["values"][s]))
        super().print_output_message(f"values_to_mean: {values_to_mean}")
        result = statistics.mean(values_to_mean)
        super().print_output_message(f"result: {result}")
        super().print_bottom()
        return PostCalculationInstruction.MOVE_TO_NEXT_VAR__VALUE_RESOLVED, result


class Calculator_Mean_N_Or_More(Calculator):
    def __init__(self, row_variable_lookup: tuple, max_count_of_missing_values: int):
        super().__init__(row_variable_lookup)
        self._max_count_of_missing_values = max_count_of_missing_values

    def evaluate(self, row_response: tuple) -> (PostCalculationInstruction, str):
        super().print_output_message("Calculator_Mean_N_Or_More: find mean from values mapped to keys in comma-separated list coming from survey_id_a.")
        super().print_output_message(f"If {self._max_count_of_missing_values} or more values are missing, empty results will be returned.")
        super().print_output_message(f"Here are the keys: {super().key_a}")
        list_of_keys_to_use_for_mean = super().key_a.split(",")
        expected_count = len(list_of_keys_to_use_for_mean)
        values_to_mean = []
        for s in list_of_keys_to_use_for_mean:
            if super().check_key(row_response["values"], s):
                if row_response["values"][s] is not None:
                    values_to_mean.append(int(row_response["values"][s]))
        super().print_output_message(f"values_to_mean: {values_to_mean}")
        if (len(list_of_keys_to_use_for_mean) - len(values_to_mean)) >= self._max_count_of_missing_values:
            result = ""
        else:
            result = statistics.mean(values_to_mean)
        super().print_output_message(f"result: {result}")
        super().print_bottom()
        return PostCalculationInstruction.MOVE_TO_NEXT_VAR__VALUE_RESOLVED, result


class Calculator_Mean_SkipNA(Calculator):
    def __init__(self, row_variable_lookup: tuple):
        super().__init__(row_variable_lookup)

    def evaluate(self, row_response: tuple) -> (PostCalculationInstruction, str):
        super().print_output_message("Calculator_Mean_SkipNA: find mean from values mapped to keys in comma-separated list coming from survey_id_a, skip na")
        super().print_output_message(f"Here are the keys: {super().key_a}")
        list_of_keys_to_use_for_sum = super().key_a.split(",")
        values_to_mean = []
        for s in list_of_keys_to_use_for_sum:
            print(type(row_response["values"][s]))
            print(row_response["values"][s])
            if (str(row_response["values"][s])).isnumeric():
                if row_response["values"][s] != -99:
                    values_to_mean.append(float(row_response["values"][s]))
            else:
                super().print_output_message(f"value for {s} is not numeric, returning an empty result")
                super().print_output_message(f"result: ")
                return PostCalculationInstruction.MOVE_TO_NEXT_VAR__VALUE_RESOLVED, ""
        super().print_output_message(f"values_to_mean: {values_to_mean}")
        if len(values_to_mean) > 0:
            result = statistics.mean(values_to_mean)
        else:
            result = ""
        super().print_output_message(f"result: {result}")
        return PostCalculationInstruction.MOVE_TO_NEXT_VAR__VALUE_RESOLVED, result


class Calculator_Merge(Calculator):
    def __init__(self, row_variable_lookup: tuple):
        super().__init__(row_variable_lookup)

    def evaluate(self, row_response: tuple) -> (PostCalculationInstruction, str):
        super().print_output_message("Calculator_Merge: concatenates string values from two fields")
        super().print_output_message(f"Values come from these fields: {super().key_a} and {super().key_b}")

        value_1 = str(row_response["values"][super().key_a])
        value_2 = str(row_response["values"][super().key_b])
        result = value_1 + value_2
        super().print_output_message(f"result: {result}")
        return PostCalculationInstruction.MOVE_TO_NEXT_VAR__VALUE_RESOLVED, result


class Calculator__MultiConditionalAnd_Equal_IsNull(Calculator):
    def __init__(self, row_variable_lookup: tuple):
        super().__init__(row_variable_lookup)

    def evaluate(self, row_response: tuple) -> (PostCalculationInstruction, str):
        actual_value_in_the_response_1 = str(row_response["values"][super().key_a])
        value_to_compare_with_1 = str(super().value_a)
        actual_value_in_the_response_2 = str(row_response["values"][super().key_b])
        super().print_output_message(
            f"Calculator__MultiConditionalAnd_Equal_IsNull; key_to_find_1: {super().key_a}, key_to_find_2: {super().key_b}; "
            f"formula: if actual_value_in_the_response1 == value_to_compare_with_1 and actual_value_in_the_response2 is null then new_var_value else else_value; "
            f"if {actual_value_in_the_response_1} == {value_to_compare_with_1} and {actual_value_in_the_response_2} is None then {super().new_var_value} else {super().else_value}")
        if super().isfloat(actual_value_in_the_response_1) and super().isfloat(value_to_compare_with_1):
            if float(actual_value_in_the_response_1) == float(value_to_compare_with_1) and actual_value_in_the_response_2 is None:
                return super().handle_new_var_value()
            else:
                return super().handle_else_value()
        return PostCalculationInstruction.MOVE_TO_NEXT_VAR__UNDERLYING_DATA_NOT_FOUND, ""


class Calculator__MultiConditional_Equal_Equal(Calculator):
    def __init__(self, row_variable_lookup: tuple):
        super().__init__(row_variable_lookup)

    def evaluate(self, row_response: tuple) -> (PostCalculationInstruction, str):
        actual_value_in_the_response_1 = str(row_response["values"][super().key_a])
        value_to_compare_with_1 = str(super().value_a)
        actual_value_in_the_response_2 = str(row_response["values"][super().key_b])
        value_to_compare_with_2 = str(super().value_b)
        super().print_output_message(
            f"Calculator__MultiConditional_Equal_Equal; key_to_find_1: {super().key_a}, key_to_find_2: {super().key_b}; "
            f"formula: if actual_value_in_the_response1 == value_to_compare_with_1 and actual_value_in_the_response2 == value_to_compare_with_2 then new_var_value else else_value; "
            f"if {actual_value_in_the_response_1} == {value_to_compare_with_1} and {actual_value_in_the_response_2} == {value_to_compare_with_2} then {super().new_var_value} else {super().else_value}")
        if super().isfloat(actual_value_in_the_response_1) and super().isfloat(value_to_compare_with_1) and super().isfloat(actual_value_in_the_response_2) and super().isfloat(value_to_compare_with_2):
            if float(actual_value_in_the_response_1) == float(value_to_compare_with_1) and float(actual_value_in_the_response_2) == float(value_to_compare_with_2):
                return super().handle_new_var_value()
            else:
                return super().handle_else_value()
        return PostCalculationInstruction.MOVE_TO_NEXT_VAR__UNDERLYING_DATA_NOT_FOUND, ""


class Calculator__MultiConditional_Equal_GreaterThan(Calculator):
    def __init__(self, row_variable_lookup: tuple):
        super().__init__(row_variable_lookup)

    def evaluate(self, row_response: tuple) -> (PostCalculationInstruction, str):
        actual_value_in_the_response_1 = str(row_response["values"][super().key_a])
        value_to_compare_with_1 = str(super().value_a)
        actual_value_in_the_response_2 = str(row_response["values"][super().key_b])
        value_to_compare_with_2 = str(super().value_b)

        super().print_output_message(
            f"Calculator__MultiConditional_Equal_GreaterThan; key_to_find_1: {super().key_a}, key_to_find_2: {super().key_b}; "
            f"formula: if actual_value_in_the_response1 == value_to_compare_with_1 and actual_value_in_the_response2 > value_to_compare_with_2 then new_var_value else else_value; "
            f"if {actual_value_in_the_response_1} == {value_to_compare_with_1} and {actual_value_in_the_response_2} > {value_to_compare_with_2} then {super().new_var_value} else {super().else_value}")

        if super().isfloat(actual_value_in_the_response_1) and super().isfloat(value_to_compare_with_1) and super().isfloat(actual_value_in_the_response_2) and super().isfloat(value_to_compare_with_2):
            if float(actual_value_in_the_response_1) == float(value_to_compare_with_1) and float(actual_value_in_the_response_2) > float(value_to_compare_with_2):
                return super().handle_new_var_value()
            else:
                return super().handle_else_value()
        return PostCalculationInstruction.MOVE_TO_NEXT_VAR__UNDERLYING_DATA_NOT_FOUND, ""


class Calculator__MultiConditional_Equal_IsNull(Calculator):
    def __init__(self, row_variable_lookup: tuple):
        super().__init__(row_variable_lookup)

    def evaluate(self, row_response: tuple) -> (PostCalculationInstruction, str):
        actual_value_in_the_response_1 = str(row_response["values"][super().key_a])
        value_to_compare_with_1 = str(super().value_a)
        actual_value_in_the_response_2 = str(row_response["values"][super().key_b])

        super().print_output_message(
            f"Calculator__MultiConditional_Equal_IsNull; key_to_find_1: {super().key_a}, key_to_find_2: {super().key_b}; "
            f"formula: if actual_value_in_the_response1 == value_to_compare_with_1 and actual_value_in_the_response2 is None then new_var_value else else_value; "
            f"if {actual_value_in_the_response_1} == {value_to_compare_with_1} and {actual_value_in_the_response_2} is None then {super().new_var_value} else {super().else_value}")

        if super().isfloat(actual_value_in_the_response_1) and super().isfloat(value_to_compare_with_1):
            if float(actual_value_in_the_response_1) == float(value_to_compare_with_1) and "".__eq__(actual_value_in_the_response_2):               
              return super().handle_new_var_value()
            else:
              return super().handle_else_value()
        return PostCalculationInstruction.MOVE_TO_NEXT_VAR__UNDERLYING_DATA_NOT_FOUND, ""


class Calculator__MultiConditionalAnd_Equal_Equal(Calculator):
    def __init__(self, row_variable_lookup: tuple):
        super().__init__(row_variable_lookup)

    def evaluate(self, row_response: tuple) -> (PostCalculationInstruction, str):
        actual_value_in_the_response_1 = str(row_response["values"][super().key_a])
        value_to_compare_with_1 = str(super().value_a)
        actual_value_in_the_response_2 = str(row_response["values"][super().key_b])
        value_to_compare_with_2 = str(super().value_b)

        super().print_output_message(
            f"Calculator__MultiConditionalAnd_Equal_Equal; key_to_find_1: {super().key_a}, key_to_find_2: {super().key_b}; "
            f"formula: if actual_value_in_the_response1 == value_to_compare_with_1 and actual_value_in_the_response2 == value_to_compare_with_2 then new_var_value else else_value; "
            f"if {actual_value_in_the_response_1} == {value_to_compare_with_1} and {actual_value_in_the_response_2} == {value_to_compare_with_2} then {super().new_var_value} else {super().else_value}")

        if super().isfloat(actual_value_in_the_response_1) and super().isfloat(value_to_compare_with_1) and super().isfloat(actual_value_in_the_response_2) and super().isfloat(value_to_compare_with_2):
            if float(actual_value_in_the_response_1) == float(value_to_compare_with_1) and float(actual_value_in_the_response_2) == float(value_to_compare_with_2):
                return super().handle_new_var_value()
            else:
                return super().handle_else_value()
        return PostCalculationInstruction.MOVE_TO_NEXT_VAR__UNDERLYING_DATA_NOT_FOUND, ""


class Calculator__MultiConditionalAnd_LessThan_Equal(Calculator):
    def __init__(self, row_variable_lookup: tuple):
        super().__init__(row_variable_lookup)

    def evaluate(self, row_response: tuple) -> (PostCalculationInstruction, str):
        actual_value_in_the_response_1 = str(row_response["values"][super().key_a])
        value_to_compare_with_1 = str(super().value_a)
        actual_value_in_the_response_2 = str(row_response["values"][super().key_b])
        value_to_compare_with_2 = str(super().value_b)

        super().print_output_message(
            f"Calculator__MultiConditionalAnd_LessThan_Equal; key_to_find_1: {super().key_a}, key_to_find_2: {super().key_b} ; "
            f"formula: if actual_value_in_the_response1 < value_to_compare_with_1 and actual_value_in_the_response2 == value_to_compare_with_2 then new_var_value else else_value; "
            f"if {actual_value_in_the_response_1} < {value_to_compare_with_1} and {actual_value_in_the_response_2} == {value_to_compare_with_2} then {super().new_var_value} else {super().else_value}")
        
        if super().isfloat(actual_value_in_the_response_1) and super().isfloat(value_to_compare_with_1) and super().isfloat(actual_value_in_the_response_2) and super().isfloat(value_to_compare_with_2):
            if float(actual_value_in_the_response_1) < float(value_to_compare_with_1) and float(actual_value_in_the_response_2) == float(value_to_compare_with_2):
                return super().handle_new_var_value()
            else:
                return super().handle_else_value()
        return PostCalculationInstruction.MOVE_TO_NEXT_VAR__UNDERLYING_DATA_NOT_FOUND, ""


class Calculator__None(Calculator):
    def __init__(self, row_variable_lookup: tuple):
        super().__init__(row_variable_lookup)

    def evaluate(self, row_response: tuple) -> (PostCalculationInstruction, str):
        super().print_output_message("Calculator__None: concatenates string values from two fields")
        super().print_output_message(f"Values come from these fields: {super().key_a}")
        value_1 = str(row_response["values"][super().key_a])
        result = value_1
        super().print_output_message(f"result: {result}")
        return PostCalculationInstruction.MOVE_TO_NEXT_VAR__VALUE_RESOLVED, result


class CalculatorNull(Calculator):
    def __init__(self, row_variable_lookup: tuple):
        super().__init__(row_variable_lookup)

    def evaluate(self, row_response: tuple) -> (PostCalculationInstruction, str):
        raise Exception(f"Encountered NULL Calculator: {super().action} and {super().detail}; Encountered Null Calculator. Process is stopping...")


class CalculatorPassthrough(Calculator):
    def __init__(self, row_variable_lookup: tuple):
        super().__init__(row_variable_lookup)

    def evaluate(self, row_response: tuple) -> (PostCalculationInstruction, str):
        return PostCalculationInstruction.MOVE_TO_NEXT_VAR__WILL_ATTEMPT_TO_CALCULATE_ON_THE_NEXT_PASS, ""


class CalculatorAllDone(Calculator):
    def __init__(self, row_variable_lookup: tuple):
        super().__init__(row_variable_lookup)

    def evaluate(self, row_response: tuple) -> (PostCalculationInstruction, str):
        return PostCalculationInstruction.STOP__ALL_DONE, ""


class Calculator_Recode(Calculator):
    def __init__(self, row_variable_lookup: tuple):
        super().__init__(row_variable_lookup)

    def evaluate(self, row_response: tuple) -> (PostCalculationInstruction, str):
        super().print_output_message("Calculator_Recode: 6 - value")
        super().print_output_message(f"6 - {super().key_a}")
        value_1 = str(row_response["values"][super().key_a])
        if value_1.isnumeric():
            result = 6 - int(value_1)
        super().print_output_message(f"result: {result}")
        return PostCalculationInstruction.MOVE_TO_NEXT_VAR__VALUE_RESOLVED, result


class Calculator_Recode_2(Calculator):
    def __init__(self, row_variable_lookup: tuple):
        super().__init__(row_variable_lookup)

    def evaluate(self, row_response: tuple) -> (PostCalculationInstruction, str):
        super().print_output_message("Calculator_Recode_2: 7 - value + 1")
        super().print_output_message(f"8 - {super().key_a}")
        value_1 = str(row_response["values"][super().key_a])
        if value_1.isnumeric():
            result = 6 - int(value_1)
        super().print_output_message(f"result: {result}")
        return PostCalculationInstruction.MOVE_TO_NEXT_VAR__VALUE_RESOLVED, result


class Calculator_Recode_3(Calculator):
    def __init__(self, row_variable_lookup: tuple):
        super().__init__(row_variable_lookup)

    def evaluate(self, row_response: tuple) -> (PostCalculationInstruction, str):
        super().print_output_message("Calculator_Recode: 5 - value + 1")
        super().print_output_message(f"6 - {super().key_a}")
        value_1 = str(row_response["values"][super().key_a])
        if value_1.isnumeric():
            result = 6 - int(value_1)
        super().print_output_message(f"result: {result}")
        return PostCalculationInstruction.MOVE_TO_NEXT_VAR__VALUE_RESOLVED, result


class Calculator_Subtraction(Calculator):
    def __init__(self, row_variable_lookup: tuple):
        super().__init__(row_variable_lookup)

    def evaluate(self, row_response: tuple) -> (PostCalculationInstruction, str):
        super().print_output_message(f"Calculator_Subtraction: subtract value contained in field {super().key_b} from value contained in field {super().key_a}")

        value_1 = float(row_response["values"][super().key_a])
        value_2 = float(row_response["values"][super().key_b])

        result = value_1 - value_2
        super().print_output_message(f"result: {result}")
        return PostCalculationInstruction.MOVE_TO_NEXT_VAR__VALUE_RESOLVED, result


class Calculator_Sum(Calculator):
    def __init__(self, row_variable_lookup: tuple):
        super().__init__(row_variable_lookup)

    def evaluate(self, row_response: tuple) -> (PostCalculationInstruction, str):
        super().print_output_message("Calculator_Sum: add values in comma-separated list coming from survey_id_a")
        super().print_output_message(f"Values come from these fields: {super().key_a}")
        list_of_keys_to_use_for_sum = list(set(super().key_a.split(",")))

        values_to_add = []
        for x in list_of_keys_to_use_for_sum:
            if super().check_key(row_response["values"], x):
                for s in super().convert_str_to_list(row_response["values"][x]):
                    values_to_add.append(float(s))
        super().print_output_message(f"values_to_add: {values_to_add}")
        result = ""
        if len(values_to_add) > 0:
            result = sum(values_to_add)
        super().print_output_message(f"result: {result}")
        return PostCalculationInstruction.MOVE_TO_NEXT_VAR__VALUE_RESOLVED, result


class Calculator__MultiConditionalAnd_Equal4(Calculator):
    def __init__(self, row_variable_lookup: tuple):
        super().__init__(row_variable_lookup)

    def evaluate(self, row_response: tuple) -> (PostCalculationInstruction, str):
        actual_value_in_the_response_1 = float(row_response["values"][super().key_a])
        value_to_compare_with_1 = float(super().value_a)
        actual_value_in_the_response_2 = float(row_response["values"][super().key_b])
        value_to_compare_with_2 = float(super().value_b)
        actual_value_in_the_response_3 = float(row_response["values"][super().key_c])
        value_to_compare_with_3 = float(super().value_c)
        actual_value_in_the_response_4 = float(row_response["values"][super().key_d])
        value_to_compare_with_4 = float(super().value_d)
        super().print_output_message(
            f"Calculator__MultiConditionalAnd_Equal4; key_to_find_1: {super().key_a}, key_to_find_2: {super().key_b}, key_to_find_3: {super().key_c}, key_to_find_4: {super().key_d}; "
            f"formula: if actual_value_in_the_response1 == value_to_compare_with_1 "
            f" and actual_value_in_the_response2 == value_to_compare_with_2 "
            f" and actual_value_in_the_response3 == value_to_compare_with_3 "
            f" and actual_value_in_the_response4 == value_to_compare_with_4 "
            f"then new_var_value else else_value; "
            f"if {actual_value_in_the_response_1} == {value_to_compare_with_1} "
            f"and {actual_value_in_the_response_2} == {value_to_compare_with_2} "
            f"and {actual_value_in_the_response_3} == {value_to_compare_with_3} "
            f"and {actual_value_in_the_response_4} == {value_to_compare_with_4} "
            f"then {super().new_var_value} else {super().else_value}")
        if actual_value_in_the_response_1 == value_to_compare_with_1 \
                and actual_value_in_the_response_2 == value_to_compare_with_2 \
                and actual_value_in_the_response_3 == value_to_compare_with_3 \
                and actual_value_in_the_response_4 == value_to_compare_with_4:
            return super().handle_new_var_value()
        else:
            return super().handle_else_value()


class Calculator__MultiConditionalAnd_IsIn_Equal_Equal(Calculator):
    def __init__(self, row_variable_lookup: tuple):
        super().__init__(row_variable_lookup)

    def evaluate(self, row_response: tuple) -> (PostCalculationInstruction, str):
        actual_value_in_the_response_1 = float(row_response["values"][super().key_a])
        value_to_compare_with_1 = str(super().value_a)
        list_to_use_for_comparison = value_to_compare_with_1.split(",")

        actual_value_in_the_response_2 = float(row_response["values"][super().key_b])
        value_to_compare_with_2 = float(super().value_b)

        actual_value_in_the_response_3 = float(row_response["values"][super().key_c])
        value_to_compare_with_3 = float(super().value_c)
        super().print_output_message(
            f"Calculator__MultiConditionalAnd_IsIn_Equal_Equal; key_to_find_1: {super().key_a}, key_to_find_2: {super().key_b}, key_to_find_3: {super().key_c}; "
            f"formula: if actual_value_in_the_response1 in list_to_use_for_comparison "
            f" and actual_value_in_the_response2 == value_to_compare_with_2 "
            f" and actual_value_in_the_response3 == value_to_compare_with_3 "
            f"then new_var_value else else_value; "
            f"if {actual_value_in_the_response_1} in {list_to_use_for_comparison} "
            f"and {actual_value_in_the_response_2} == {value_to_compare_with_2} "
            f"and {actual_value_in_the_response_3} == {value_to_compare_with_3} "
            f"then {super().new_var_value} else {super().else_value}")
        if actual_value_in_the_response_1 in list_to_use_for_comparison \
                and actual_value_in_the_response_2 == value_to_compare_with_2 \
                and actual_value_in_the_response_3 == value_to_compare_with_3:
            return super().handle_new_var_value()
        else:
            return super().handle_else_value()


class Calculator__Conditional_Between_Including(Calculator):
    def __init__(self, row_variable_lookup: tuple):
        super().__init__(row_variable_lookup)

    def evaluate(self, row_response: tuple) -> (PostCalculationInstruction, str):
        actual_value_in_the_response = str(super().actual_value_in_response_a(row_response))
        value_to_compare_with_1 = str(super().value_a)
        value_to_compare_with_2 = str(super().value_a2)
        super().print_output_message(
            f"Calculator__Conditional_Between_Including; key_to_find: {super().key_a}; "
            f"formula: if actual_value_in_the_response >= value_to_compare_with_1 "
            f"and actual_value_in_the_response <= value_to_compare_with_2"
            f"then new_var_value else else_value; "
            f"if {actual_value_in_the_response} >= {value_to_compare_with_1} and "
            f"{actual_value_in_the_response} <= {value_to_compare_with_2}"
            f" then {super().new_var_value} else {super().else_value}")
        if value_to_compare_with_1 <= actual_value_in_the_response <= value_to_compare_with_2:
            return super().handle_new_var_value()
        else:
            return super().handle_else_value()


class Calculator__MultiConditionalAnd_Equal_GreaterThan(Calculator):
    def __init__(self, row_variable_lookup: tuple):
        super().__init__(row_variable_lookup)

    def evaluate(self, row_response: tuple) -> (PostCalculationInstruction, str):
        actual_value_in_the_response_1 = row_response["values"][super().key_a]
        value_to_compare_with_1 = super().value_a
        actual_value_in_the_response_2 = row_response["values"][super().key_b]
        value_to_compare_with_2 = super().value_b

        super().print_output_message(
            f"Calculator__MultiConditionalAnd_Equal_GreaterThan; key_to_find_1: {super().key_a}, key_to_find_2: {super().key_b}; "
            f"formula: if actual_value_in_the_response1 == value_to_compare_with_1 and actual_value_in_the_response2 > value_to_compare_with_2 then new_var_value else else_value; "
            f"if {actual_value_in_the_response_1} == {value_to_compare_with_1} and {actual_value_in_the_response_2} > {value_to_compare_with_2} then {super().new_var_value} else {super().else_value}")

        if super().isfloat(actual_value_in_the_response_1) and super().isfloat(value_to_compare_with_1) and super().isfloat(actual_value_in_the_response_2) and super(value_to_compare_with_2).isfloat():
            if float(actual_value_in_the_response_1) == float(value_to_compare_with_1) and float(actual_value_in_the_response_2) > float(value_to_compare_with_2):
                return super().handle_new_var_value()
            else:
                return super().handle_else_value()
        else:
            return PostCalculationInstruction.MOVE_TO_NEXT_VAR__UNDERLYING_DATA_NOT_FOUND, ""


class Calculator__MultiConditionalAnd_IsIn_IsIn(Calculator):
    def __init__(self, row_variable_lookup: tuple):
        super().__init__(row_variable_lookup)

    def evaluate(self, row_response: tuple) -> (PostCalculationInstruction, str):
        actual_value_in_the_response_1 = float(row_response["values"][super().key_a])
        value_to_compare_with_1 = str(super().value_a)
        list_to_use_for_comparison_1 = value_to_compare_with_1.split(",")

        actual_value_in_the_response_2 = float(row_response["values"][super().key_b])
        value_to_compare_with_2 = str(super().value_b)
        list_to_use_for_comparison_2 = value_to_compare_with_2.split(",")

        super().print_output_message(
            f"Calculator__MultiConditionalAnd_IsIn_IsIn; key_to_find_1: {super().key_a}, key_to_find_2: {super().key_b};"
            f"formula: if actual_value_in_the_response1 in list_to_use_for_comparison_1 "
            f" and actual_value_in_the_response2 in list_to_use_for_comparison_2 "
            f"then new_var_value else else_value; "
            f"if {actual_value_in_the_response_1} in {list_to_use_for_comparison_1} "
            f"and {actual_value_in_the_response_2} in {list_to_use_for_comparison_2} "
            f"then {super().new_var_value} else {super().else_value}")
        if actual_value_in_the_response_1 in list_to_use_for_comparison_1 \
                and actual_value_in_the_response_2 in list_to_use_for_comparison_2:
            return super().handle_new_var_value()
        else:
            return super().handle_else_value()


class Calculator__MultiConditionalAnd_GreaterThanEqual_GreaterThan(Calculator):
    def __init__(self, row_variable_lookup: tuple):
        super().__init__(row_variable_lookup)

    def evaluate(self, row_response: tuple) -> (PostCalculationInstruction, str):
        actual_value_in_the_response_1 = row_response["values"][super().key_a]
        value_to_compare_with_1 = super().value_a
        actual_value_in_the_response_2 = row_response["values"][super().key_b]
        value_to_compare_with_2 = super().value_b
        super().print_output_message(
            f"Calculator__MultiConditionalAnd_Equal_GreaterThan; key_to_find_1: {super().key_a}, key_to_find_2: {super().key_b}; "
            f"formula: if actual_value_in_the_response1 == value_to_compare_with_1 and actual_value_in_the_response2 > value_to_compare_with_2 then new_var_value else else_value; "
            f"if {actual_value_in_the_response_1} == {value_to_compare_with_1} and {actual_value_in_the_response_2} > {value_to_compare_with_2} then {super().new_var_value} else {super().else_value}")

        if super().isfloat(actual_value_in_the_response_1) and super().isfloat(value_to_compare_with_1) and super().isfloat(actual_value_in_the_response_2) and super().isfloat(value_to_compare_with_2):
            if float(actual_value_in_the_response_1) == float(value_to_compare_with_1) and float(actual_value_in_the_response_2) > float(value_to_compare_with_2):
                return super().handle_new_var_value()
            else:
                return super().handle_else_value()
        else:
            return PostCalculationInstruction.MOVE_TO_NEXT_VAR__UNDERLYING_DATA_NOT_FOUND, ""


class Calculator__Product(Calculator):
    def __init__(self, row_variable_lookup: tuple):
        super().__init__(row_variable_lookup)

    def evaluate(self, row_response: tuple) -> (PostCalculationInstruction, str):
        actual_value_in_the_response = str(super().actual_value_in_response_a(row_response))
        value_to_multiply_by = str(super().value_a)
        super().print_output_message(
            f"Calculator__Product; key_to_find: {super().key_a}; "
            f"formula: returns actual_value_in_the_response * value_to_multiply_by"
            f"{actual_value_in_the_response} * {value_to_multiply_by}")

        if super().isfloat(actual_value_in_the_response) and super().isfloat(value_to_multiply_by):
            result = float(actual_value_in_the_response) * float(value_to_multiply_by)
        else:
            result = ""
        super().print_output_message(f"result: {result}")
        return PostCalculationInstruction.MOVE_TO_NEXT_VAR__VALUE_RESOLVED, result


class Calculator__Conditional_IsNull(Calculator):
    def __init__(self, row_variable_lookup: tuple):
        super().__init__(row_variable_lookup)

    def evaluate(self, row_response: tuple) -> (PostCalculationInstruction, str):
        actual_value_in_the_response_1 = str(row_response["values"][super().key_a])
        super().print_output_message(
            f"Calculator__Conditional_IsNull; key_to_find_1: {super().key_a}; "
            f"formula: if actual_value_in_the_response1 is null then new_var_value else else_value; "
            f"if {actual_value_in_the_response_1} is None then {super().new_var_value} else {super().else_value}")        
        if actual_value_in_the_response_1 is None:
            return super().handle_new_var_value()
        else:
            return super().handle_else_value()
        return PostCalculationInstruction.MOVE_TO_NEXT_VAR__UNDERLYING_DATA_NOT_FOUND, ""
      
      
class Calculator__MultiConditionalAnd_IsIn_Equal(Calculator):
    def __init__(self, row_variable_lookup: tuple):
        super().__init__(row_variable_lookup)

    def evaluate(self, row_response: tuple) -> (PostCalculationInstruction, str):
        actual_value_in_the_response_1 = float(row_response["values"][super().key_a])
        value_to_compare_with_1 = str(super().value_a)
        list_to_use_for_comparison = value_to_compare_with_1.split(",")

        actual_value_in_the_response_2 = float(row_response["values"][super().key_b])
        value_to_compare_with_2 = float(super().value_b)
        
        super().print_output_message(
            f"Calculator__MultiConditionalAnd_IsIn_Equal; key_to_find_1: {super().key_a}, key_to_find_2: {super().key_b};"
            f"formula: if actual_value_in_the_response1 in list_to_use_for_comparison "
            f" and actual_value_in_the_response2 == value_to_compare_with_2 "
            f"then new_var_value else else_value; "
            f"if {actual_value_in_the_response_1} in {list_to_use_for_comparison} "
            f"and {actual_value_in_the_response_2} == {value_to_compare_with_2} "
            f"then {super().new_var_value} else {super().else_value}")
        if actual_value_in_the_response_1 in list_to_use_for_comparison \
                and actual_value_in_the_response_2 == value_to_compare_with_2:
            return super().handle_new_var_value()
        else:
            return super().handle_else_value()
      

class Calculator__MultiConditionalAnd_Equal_LessThan_LessThen(Calculator):
    def __init__(self, row_variable_lookup: tuple):
        super().__init__(row_variable_lookup)

    def evaluate(self, row_response: tuple) -> (PostCalculationInstruction, str):
        actual_value_in_the_response_1 = row_response["values"][super().key_a]
        value_to_compare_with_1 = super().value_a
        actual_value_in_the_response_2 = row_response["values"][super().key_b]
        value_to_compare_with_2 = super().value_b
        actual_value_in_the_response_3 = row_response["values"][super().key_c]
        value_to_compare_with_3 = super().value_c
        super().print_output_message(
            f"Calculator__MultiConditionalAnd_Equal_LessThan_LessThen; key_to_find_1: {super().key_a}, key_to_find_2: {super().key_b}, key_to_find_3: {super().key_c}; "
            f"formula: if actual_value_in_the_response1 == value_to_compare_with_1 and actual_value_in_the_response2 < value_to_compare_with_2 and actual_value_in_the_response3 < value_to_compare_with_3 then new_var_value else else_value; "
            f"if {actual_value_in_the_response_1} == {value_to_compare_with_1} and {actual_value_in_the_response_2} < {value_to_compare_with_2} and {actual_value_in_the_response_3} < {value_to_compare_with_3} then {super().new_var_value} else {super().else_value}")

        if super().isfloat(actual_value_in_the_response_1) and super().isfloat(value_to_compare_with_1) and super().isfloat(actual_value_in_the_response_2) and super().isfloat(value_to_compare_with_2) and super().isfloat(actual_value_in_the_response_3) and super().isfloat(value_to_compare_with_3):
            if float(actual_value_in_the_response_1) == float(value_to_compare_with_1) and float(actual_value_in_the_response_2) < float(value_to_compare_with_2) and float(actual_value_in_the_response_3) < float(value_to_compare_with_3):
                return super().handle_new_var_value()
            else:
                return super().handle_else_value()
        else:
            return PostCalculationInstruction.MOVE_TO_NEXT_VAR__UNDERLYING_DATA_NOT_FOUND, "" 


class Calculator__MultiConditionalAnd_Equal_GreaterThanEqual(Calculator):
    def __init__(self, row_variable_lookup: tuple):
        super().__init__(row_variable_lookup)

    def evaluate(self, row_response: tuple) -> (PostCalculationInstruction, str):
        actual_value_in_the_response_1 = row_response["values"][super().key_a]
        value_to_compare_with_1 = super().value_a
        actual_value_in_the_response_2 = row_response["values"][super().key_b]
        value_to_compare_with_2 = super().value_b

        super().print_output_message(
            f"Calculator__MultiConditionalAnd_Equal_GreaterThanEqual; key_to_find_1: {super().key_a}, key_to_find_2: {super().key_b}; "
            f"formula: if actual_value_in_the_response1 == value_to_compare_with_1 and actual_value_in_the_response2 >= value_to_compare_with_2 then new_var_value else else_value; "
            f"if {actual_value_in_the_response_1} == {value_to_compare_with_1} and {actual_value_in_the_response_2} >= {value_to_compare_with_2} then {super().new_var_value} else {super().else_value}")

        if super().isfloat(actual_value_in_the_response_1) and super().isfloat(value_to_compare_with_1) and super().isfloat(actual_value_in_the_response_2) and super(value_to_compare_with_2).isfloat():
            if float(actual_value_in_the_response_1) == float(value_to_compare_with_1) and float(actual_value_in_the_response_2) >= float(value_to_compare_with_2):
                return super().handle_new_var_value()
            else:
                return super().handle_else_value()
        else:
            return PostCalculationInstruction.MOVE_TO_NEXT_VAR__UNDERLYING_DATA_NOT_FOUND, ""


class Calculator__MultiConditionalAnd_Equal_LessThan(Calculator):
    def __init__(self, row_variable_lookup: tuple):
        super().__init__(row_variable_lookup)

    def evaluate(self, row_response: tuple) -> (PostCalculationInstruction, str):
        actual_value_in_the_response_1 = row_response["values"][super().key_a]
        value_to_compare_with_1 = super().value_a
        actual_value_in_the_response_2 = row_response["values"][super().key_b]
        value_to_compare_with_2 = super().value_b

        super().print_output_message(
            f"Calculator__MultiConditionalAnd_Equal_LessThan; key_to_find_1: {super().key_a}, key_to_find_2: {super().key_b}; "
            f"formula: if actual_value_in_the_response1 == value_to_compare_with_1 and actual_value_in_the_response2 < value_to_compare_with_2 then new_var_value else else_value; "
            f"if {actual_value_in_the_response_1} == {value_to_compare_with_1} and {actual_value_in_the_response_2} < {value_to_compare_with_2} then {super().new_var_value} else {super().else_value}")

        if super().isfloat(actual_value_in_the_response_1) and super().isfloat(value_to_compare_with_1) and super().isfloat(actual_value_in_the_response_2) and super(value_to_compare_with_2).isfloat():
            if float(actual_value_in_the_response_1) == float(value_to_compare_with_1) and float(actual_value_in_the_response_2) < float(value_to_compare_with_2):
                return super().handle_new_var_value()
            else:
                return super().handle_else_value()
        else:
            return PostCalculationInstruction.MOVE_TO_NEXT_VAR__UNDERLYING_DATA_NOT_FOUND, ""


class Calculator__MultiConditionalAnd_LessThan_GreaterThanEqual(Calculator):
    def __init__(self, row_variable_lookup: tuple):
        super().__init__(row_variable_lookup)

    def evaluate(self, row_response: tuple) -> (PostCalculationInstruction, str):
        actual_value_in_the_response_1 = row_response["values"][super().key_a]
        value_to_compare_with_1 = super().value_a
        actual_value_in_the_response_2 = row_response["values"][super().key_b]
        value_to_compare_with_2 = super().value_b

        super().print_output_message(
            f"Calculator__MultiConditionalAnd_LessThan_GreaterThanEqual; key_to_find_1: {super().key_a}, key_to_find_2: {super().key_b}; "
            f"formula: if actual_value_in_the_response1 < value_to_compare_with_1 and actual_value_in_the_response2 >= value_to_compare_with_2 then new_var_value else else_value; "
            f"if {actual_value_in_the_response_1} < {value_to_compare_with_1} and {actual_value_in_the_response_2} >= {value_to_compare_with_2} then {super().new_var_value} else {super().else_value}")

        if super().isfloat(actual_value_in_the_response_1) and super().isfloat(value_to_compare_with_1) and super().isfloat(actual_value_in_the_response_2) and super(value_to_compare_with_2).isfloat():
            if float(actual_value_in_the_response_1) < float(value_to_compare_with_1) and float(actual_value_in_the_response_2) >= float(value_to_compare_with_2):
                return super().handle_new_var_value()
            else:
                return super().handle_else_value()
        else:
            return PostCalculationInstruction.MOVE_TO_NEXT_VAR__UNDERLYING_DATA_NOT_FOUND, ""


class Calculator__MultiConditionalAnd_LessThan_LessThan(Calculator):
    def __init__(self, row_variable_lookup: tuple):
        super().__init__(row_variable_lookup)

    def evaluate(self, row_response: tuple) -> (PostCalculationInstruction, str):
        actual_value_in_the_response_1 = row_response["values"][super().key_a]
        value_to_compare_with_1 = super().value_a
        actual_value_in_the_response_2 = row_response["values"][super().key_b]
        value_to_compare_with_2 = super().value_b

        super().print_output_message(
            f"Calculator__MultiConditionalAnd_LessThan_LessThan; key_to_find_1: {super().key_a}, key_to_find_2: {super().key_b}; "
            f"formula: if actual_value_in_the_response1 < value_to_compare_with_1 and actual_value_in_the_response2 < value_to_compare_with_2 then new_var_value else else_value; "
            f"if {actual_value_in_the_response_1} < {value_to_compare_with_1} and {actual_value_in_the_response_2} < {value_to_compare_with_2} then {super().new_var_value} else {super().else_value}")

        if super().isfloat(actual_value_in_the_response_1) and super().isfloat(value_to_compare_with_1) and super().isfloat(actual_value_in_the_response_2) and super(value_to_compare_with_2).isfloat():
            if float(actual_value_in_the_response_1) < float(value_to_compare_with_1) and float(actual_value_in_the_response_2) < float(value_to_compare_with_2):
                return super().handle_new_var_value()
            else:
                return super().handle_else_value()
        else:
            return PostCalculationInstruction.MOVE_TO_NEXT_VAR__UNDERLYING_DATA_NOT_FOUND, ""


class Calculator__MultiConditionalAnd_Equal_Equal_Equal(Calculator):
    def __init__(self, row_variable_lookup: tuple):
        super().__init__(row_variable_lookup)

    def evaluate(self, row_response: tuple) -> (PostCalculationInstruction, str):
        actual_value_in_the_response_1 = row_response["values"][super().key_a]
        value_to_compare_with_1 = super().value_a
        actual_value_in_the_response_2 = row_response["values"][super().key_b]
        value_to_compare_with_2 = super().value_b
        actual_value_in_the_response_3 = row_response["values"][super().key_c]
        value_to_compare_with_3 = super().value_c
        super().print_output_message(
            f"Calculator__MultiConditionalAnd_Equal_Equal_Equal; key_to_find_1: {super().key_a}, key_to_find_2: {super().key_b}, key_to_find_3: {super().key_c}; "
            f"formula: if actual_value_in_the_response1 == value_to_compare_with_1 and actual_value_in_the_response2 == value_to_compare_with_2 and actual_value_in_the_response3 == value_to_compare_with_3 then new_var_value else else_value; "
            f"if {actual_value_in_the_response_1} == {value_to_compare_with_1} and {actual_value_in_the_response_2} == {value_to_compare_with_2} and {actual_value_in_the_response_3} == {value_to_compare_with_3} then {super().new_var_value} else {super().else_value}")

        if super().isfloat(actual_value_in_the_response_1) and super().isfloat(value_to_compare_with_1) and super().isfloat(actual_value_in_the_response_2) and super().isfloat(value_to_compare_with_2) and super().isfloat(actual_value_in_the_response_3) and super().isfloat(value_to_compare_with_3):
            if float(actual_value_in_the_response_1) == float(value_to_compare_with_1) and float(actual_value_in_the_response_2) == float(value_to_compare_with_2) and float(actual_value_in_the_response_3) == float(value_to_compare_with_3):
                return super().handle_new_var_value()
            else:
                return super().handle_else_value()
        else:
            return PostCalculationInstruction.MOVE_TO_NEXT_VAR__UNDERLYING_DATA_NOT_FOUND, ""


class Calculator__Count(Calculator):
    def __init__(self, row_variable_lookup: tuple):
        super().__init__(row_variable_lookup)

    def evaluate(self, row_response: tuple) -> (PostCalculationInstruction, str):
        actual_value_in_the_response = (str(super().actual_value_in_response_a(row_response))).split(",")
        super().print_output_message(
            f"Calculator__Count; key_to_find: {super().key_a}; "
            f"formula: returns count of items in the list")

        if not type(actual_value_in_the_response) == list:
            result = 0
        else:
            result = len(actual_value_in_the_response)

        super().print_output_message(f"result: {result}")
        return PostCalculationInstruction.MOVE_TO_NEXT_VAR__VALUE_RESOLVED, result     
  

# COMMAND ----------

# DBTITLE 1,Calculator Factory
class CalculatorFactory:
    @staticmethod
    def create_calculator(variable_lookup_row: tuple) -> Calculator:
        print(variable_lookup_row)
        action = variable_lookup_row["action"]
        detail = variable_lookup_row["detail"]
        new_var_name = variable_lookup_row["new_variable"]

        if action == "recode":
            return Calculator_Recode(variable_lookup_row)
        elif action == "recode_2":
            return Calculator_Recode_2(variable_lookup_row)
        elif action == "recode_3":
            return Calculator_Recode_3(variable_lookup_row)
        elif action in ["conditional", "conditional_2", "conditional_3"]:
            if detail == "equal":
                return Calculator__Conditional_Equal(variable_lookup_row)
            elif detail == "greater_than":
                return Calculator__Conditional_GreaterThan(variable_lookup_row)
            elif detail == "greater_than_equal":
                return Calculator_Conditional_GreaterThanEqual(variable_lookup_row)
            elif detail == "less_than":
                return Calculator_Conditional_LessThan(variable_lookup_row)
            elif detail == "less_than_equal":
                return Calculator_Conditional_LessThanEqual(variable_lookup_row)
            elif detail == "is_in":
                return Calculator__Conditional_IsIn(variable_lookup_row)
            elif detail == "between_including":
                return Calculator__Conditional_Between_Including(variable_lookup_row)
            elif detail == "is_null":
                return Calculator__Conditional_IsNull(variable_lookup_row)
            elif detail == "between_including":
                return Calculator__Conditional_Between_Including(variable_lookup_row)
            elif detail == "equal_string":
                return Calculator__Conditional_EqualString(variable_lookup_row)
            else:
                return CalculatorNull(variable_lookup_row)
        elif action == "multi_conditional":
            if detail == "equal,equal":
                return Calculator__MultiConditional_Equal_Equal(variable_lookup_row)
            elif detail == "equal,greater_than":
                return Calculator__MultiConditional_Equal_GreaterThan(variable_lookup_row)
            elif detail == "equal,is_null":
                return Calculator__MultiConditional_Equal_IsNull(variable_lookup_row)
            else:
                return CalculatorNull(variable_lookup_row)
        elif action == "multi_conditional_and":
            if detail == "equal,equal":
                return Calculator__MultiConditionalAnd_Equal_Equal(variable_lookup_row)
            if detail == "equal,is_null":
                return Calculator__MultiConditionalAnd_Equal_IsNull(variable_lookup_row)
            if detail == "less_than,equal":
                return Calculator__MultiConditionalAnd_LessThan_Equal(variable_lookup_row)
            elif detail == "is_in,equal,equal":
                return Calculator__MultiConditionalAnd_IsIn_Equal_Equal(variable_lookup_row)
            elif detail == "equal,greater_than":
                return Calculator__MultiConditionalAnd_Equal_GreaterThan(variable_lookup_row)
            elif detail == "is_in,equal":
                return Calculator__MultiConditionalAnd_IsIn_Equal(variable_lookup_row)
            elif detail == "is_in,is_in":
                return Calculator__MultiConditionalAnd_IsIn_IsIn(variable_lookup_row)
            elif detail == "greater_than_equal,greater_than":
                return Calculator__MultiConditionalAnd_GreaterThanEqual_GreaterThan(variable_lookup_row)
            else:
                return CalculatorNull(variable_lookup_row)
        elif action == "multi_conditional_and_2":
            if detail == "less_than,equal":
                return Calculator__MultiConditionalAnd_LessThan_Equal(variable_lookup_row)
            elif detail == "equal,equal,equal,equal":
                return Calculator__MultiConditionalAnd_Equal4(variable_lookup_row)
            elif detail == "greater_than_equal,greater_than":
                return Calculator__MultiConditionalAnd_GreaterThanEqual_GreaterThan(variable_lookup_row)
            elif detail == "equal,less_than,less_than":
                return Calculator__MultiConditionalAnd_Equal_LessThan_LessThen(variable_lookup_row)
            else:
                return CalculatorNull(variable_lookup_row)
        elif action == "multi_conditional_and_3":
            if detail == "equal,equal":
                return Calculator__MultiConditional_Equal_Equal(variable_lookup_row)
            elif detail == "equal,greater_than":
                return Calculator__MultiConditional_Equal_GreaterThan(variable_lookup_row)
            elif detail == "equal,greater_than_equal":
                return Calculator__MultiConditionalAnd_Equal_GreaterThanEqual(variable_lookup_row)
            elif detail == "equal,less_than":
                return Calculator__MultiConditionalAnd_Equal_LessThan(variable_lookup_row)
            elif detail == "equal,is_null":
                return Calculator__MultiConditional_Equal_IsNull(variable_lookup_row)
            elif detail == "less_than,greater_than_equal":
                return Calculator__MultiConditionalAnd_LessThan_GreaterThanEqual(variable_lookup_row)
            elif detail == "less_than,less_than":
                return Calculator__MultiConditionalAnd_LessThan_LessThan(variable_lookup_row)
            elif detail == "equal,equal,equal":
                return Calculator__MultiConditionalAnd_Equal_Equal_Equal(variable_lookup_row)
            elif detail == "less_than,equal":
                return Calculator__MultiConditionalAnd_LessThan_Equal(variable_lookup_row)
            else:
                return CalculatorNull(variable_lookup_row)
        elif action in ["sum", "sum_2", "sum_3", "sum_4"]:
            return Calculator_Sum(variable_lookup_row)
        elif action == "subtraction":
            return Calculator_Subtraction(variable_lookup_row)
        elif action == "mean":
            return Calculator_Mean(variable_lookup_row)
        elif action == "mean_2_or_more":
            return Calculator_Mean_N_Or_More(variable_lookup_row, 2)
        elif action == "mean_3_or_more":
            return Calculator_Mean_N_Or_More(variable_lookup_row, 3)
        elif action == "mean_4_or_more":
            return Calculator_Mean_N_Or_More(variable_lookup_row, 4)
        elif action == "mean_5_or_more":
            return Calculator_Mean_N_Or_More(variable_lookup_row, 5)
        elif action == "mean_skipna":
            return Calculator_Mean_SkipNA(variable_lookup_row)
        elif action == "merge":
            return Calculator_Merge(variable_lookup_row)
        elif action == "product":
            return Calculator__Product(variable_lookup_row)
        elif action is None:
            return Calculator__None(variable_lookup_row)
        elif action == "count":
            return Calculator__Count(variable_lookup_row)
        else:
            return CalculatorNull(variable_lookup_row)

