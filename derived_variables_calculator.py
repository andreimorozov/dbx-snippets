# Databricks notebook source
# MAGIC %md # Derived Variable Calculator

# COMMAND ----------

# MAGIC %md ## Overview
# MAGIC This compoment contains logic necessary to produce derived variables for survey.
# MAGIC A caller must supply with two arguments: 
# MAGIC - actual survey to be processed. The survey will have all the known responses from the serveyors.
# MAGIC - set of instruction in a form a dataframe. In this version, the instructions will be prepared by a caller from a flatfile residing in S3
# MAGIC 
# MAGIC The output will be an extended dataframe with the new derived variables.
# MAGIC The new derived variables will not be produced if the underlying data is missing, or conditions to resolve new variable value are not met and no else value provided

# COMMAND ----------

# MAGIC %md ## Bring in Calculator Engine classes

# COMMAND ----------

# MAGIC %run ./derived_variables_calculator_engine

# COMMAND ----------

# MAGIC %md ## Calculator top level classes

# COMMAND ----------

import numpy as np
import pandas as pd
from pandas import DataFrame
from typing import Iterator
from pyspark.sql.functions import col, pandas_udf, struct, PandasUDFType
from pyspark.sql.types import StructType, StructField, FloatType

class SurveyDerivedVariablesCalculator:
    
    @staticmethod
    def produce_derived_variables_dataframe(df_derived_variables_lookup: DataFrame, list_of_response_dictionaries: list) -> DataFrame:
        result = []
        for response_dict in list_of_response_dictionaries:
            single_response_survey_derived_variable_calculator = SingleResponseSurveyDerivedVariablesCalculator(df_derived_variables_lookup, response_dict)
            single_response_survey_derived_variable_calculator.is_printing_output_messages = True
            single_response_survey_derived_variable_calculator.produce_derived_variables()
            result.append(response_dict)
        return pd.DataFrame.from_dict(result)
    
    @staticmethod
    def produce_derived_variables_dataframe_for_single_response_row(df_derived_variables_lookup: DataFrame, response_dict: dict) -> dict:                
        single_response_survey_derived_variable_calculator = SingleResponseSurveyDerivedVariablesCalculator(df_derived_variables_lookup, response_dict)
        single_response_survey_derived_variable_calculator.is_printing_output_messages = True
        single_response_survey_derived_variable_calculator.produce_derived_variables()        
        return pd.DataFrame.from_dict(response_dict)
      
class SingleResponseSurveyDerivedVariablesCalculator:
    def __init__(self, df_derived_variables_lookup: DataFrame, row_response_dict: dict):
        self.df_derived_variables_lookup = df_derived_variables_lookup
        self.row_response_dict = row_response_dict
        self._is_printing_output_messages = False

    @property
    def is_printing_output_messages(self):
        return self._is_printing_output_messages

    @is_printing_output_messages.setter
    def is_printing_output_messages(self, value):
        self._is_printing_output_messages = value

    def print_output_message(self, message):
        if self._is_printing_output_messages:
            print(message)

    def produce_derived_variables(self):
        var_names = self.df_derived_variables_lookup["new_variable"].unique()
        var_names = [x for x in var_names if x is not None]
        var_names_working_copy = np.ndarray.copy(np.asarray(var_names))
        factory = CalculatorFactory()
        max_pass_number = max(set(self.df_derived_variables_lookup["pass_number"]))

        for pass_number in range(0, max_pass_number + 1):
            for var_name in var_names_working_copy:
                df_derived_variables_lookup_one_var_block_of_rows = self.df_derived_variables_lookup.loc[(self.df_derived_variables_lookup["new_variable"] == var_name) & (self.df_derived_variables_lookup["pass_number"] == pass_number)]
                
                for i in df_derived_variables_lookup_one_var_block_of_rows.index:
                    variable_lookup_row = df_derived_variables_lookup_one_var_block_of_rows.loc[i]
                    calculator = factory.create_calculator(variable_lookup_row)

                    if calculator is None:
                        raise Exception(f"action: {variable_lookup_row['action']}; detail: {variable_lookup_row['detail']}; pass_number: {pass_number}")

                    calculator.is_printing_output_messages = self._is_printing_output_messages
                    calculation_result = calculator.produce_new_var(self.row_response_dict)

                    if calculation_result[0] == PostCalculationInstruction.MOVE_TO_NEXT_VAR__VALUE_RESOLVED:
                        self.row_response_dict["values"][variable_lookup_row["new_variable"]] = calculation_result[1]
                        break
                    elif calculation_result[0] == PostCalculationInstruction.MOVE_TO_NEXT_RULE__KEYS_EXIST_CONDITIONS_NOT_MET:
#                         self.row_response_dict["values"][variable_lookup_row["new_variable"]] = ""
                        continue
                    elif calculation_result[0] == PostCalculationInstruction.MOVE_TO_NEXT_VAR__UNDERLYING_DATA_NOT_FOUND:
                        var_names_working_copy = var_names_working_copy[var_names_working_copy != var_name]
#                         self.row_response_dict["values"][variable_lookup_row["new_variable"]] = ""
                        break
                    elif calculation_result[0] == PostCalculationInstruction.MOVE_TO_NEXT_VAR__WILL_ATTEMPT_TO_CALCULATE_ON_THE_NEXT_PASS:
                        is_removing_var_name_from_var_names_working_copy = False
#                         self.row_response_dict["values"][variable_lookup_row["new_variable"]] = ""
                        break
                    elif calculation_result[0] == PostCalculationInstruction.STOP__ALL_DONE:
#                         self.row_response_dict["values"][variable_lookup_row["new_variable"]] = ""
                        return
                    else:
                        raise ValueError(f"Unsupported case: PostCalculationInstruction  = {calculation_result[0]}")
              
