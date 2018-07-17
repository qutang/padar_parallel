"""

Various Group by functions (based on dask to support not-fit-in-memory data)
 on files and dataframes

Author: Qu Tang

Date: 07/17/2018

"""
import os
import numpy as np
from itertools import product


class GroupBy:
    def __init__(self, *data_inputs):
        self._data_inputs = data_inputs
        self._groups = {}

    def split(self, *groups):
        """

        Group input by list of groups. Length should match the length of input
         list
        Results will be in a dictionary

        """

        zipped_groups = zip(self._data_inputs, *groups)

        for zipped_group in zipped_groups:
            data_input = zipped_group[0]
            groups = zipped_groups[1:]
            group_name = '-'.join(groups)
            if group_name in self._groups:
                self._groups[group_name].append(data_input)
            else:
                self._groups[group_name] = [data_input]

        return self

    @staticmethod
    def windowing(data_inputs, func, st, et, interval, step, join_adjacent):
        raise NotImplementedError("TODO")

    def apply_by_window(self, func, st, et, interval, step,
                        join_adjacent=True):
        """
        Apply function to window chunks of each group's data input. Only
         applicable when data can be imported as dataframe

        Arguments:
            func {dask.delayed} -- this function should return a dask.delayed
             object that can be called later for distributed computing
            interval {float} -- [description]
            step {[type]} -- [description]
        """

        windowing_func = Groupby.windowing(func, st, et,
                                           interval, step, join_adjacent)
        self.apply(windowing_func)
        return self

    def apply(self, func):
        """[summary]

        Arguments:
            func {[type]} -- [description]
        """
