# from snowflake.snowpark.types import IntegerType
# import snowflake.snowpark.types as snowpark_types
# from snowflake.snowpark.functions import udf, udaf
from src.typedefs import Entry
from typing import List

"""
This module contains the UDFs for the project.
"""

def combine(string_a: str, string_b: str) -> str:
    """
    A sample UDF implementation
    """

    return string_a + string_b

# Using the decorator would be nice, but it requires a default session.
# @udaf(name="combine_payloads", replace=True, return_type=snowpark_types.VariantType(), input_types=[snowpark_types.VariantType()])
class EstimationStatsCombinerUDAF:
    def __init__(self) -> None:
        self._acc: List[Entry] = []
        # self._sum = 0

    @property
    def aggregate_state(self):
        return self._acc

    def accumulate(self, input_value):
        # self._sum += 1
        self._acc.append(Entry.model_validate(input_value))
        # self._acc.append(input_value)
        # self._sum += input_value

    def merge(self, other_sum):
        self._acc.extend(other_sum)
        #self._sum += other_sum

    def finish(self):
        self._acc.sort(key=lambda e: e.elapsedTime)
        return [e.model_dump() for e in self._acc]

class PythonSumUDAF:
    def __init__(self) -> None:
        self._sum = 0

    @property
    def aggregate_state(self):
        return self._sum

    def accumulate(self, input_value):
        self._sum += 1
        # self._sum += input_value

    def merge(self, other_sum):
        self._sum += other_sum

    def finish(self):
        return self._sum