"""

Various Group by functions (based on dask to support not-fit-in-memory data)
 on files and dataframes

Author: Qu Tang

Date: 07/17/2018

"""
import os
import numpy as np
import pprint
import copy
import pandas as pd
from dask import delayed
import dask


class GroupBy:
    def __init__(self, *data_inputs):
        self._data_inputs = data_inputs
        self._groups = {}
        self._applied_groups = {}

    def split(self, *groups, ingroup_sortkey_func=None, descending=False):
        """

        Group input by list of groups. Length should match the length of input
         list
        Results will be in a dictionary

        """

        zipped_groups = list(zip(self._data_inputs, *groups))

        # assign to groups
        for zipped_group in zipped_groups:
            data_input = zipped_group[0]
            groups = zipped_group[1:]
            group_name = '-'.join(groups)
            if group_name in self._groups:
                self._groups[group_name].append(data_input)
            else:
                self._groups[group_name] = [data_input]

        # sort items in each group
        for group_name in self._groups:
            if ingroup_sortkey_func is None:
                self._groups[group_name].sort()
            else:
                self._groups[group_name].sort(
                    key=ingroup_sortkey_func, reverse=descending)

        return self

    def post_join(self, join_func=None):
        for group_name in self._applied_groups:
            if join_func is not None:
                self._applied_groups[group_name] = join_func(
                    self._applied_groups[group_name])
        return self

    def final_join(self, join_func=None):
        self._joined_applied_groups = join_func(self._applied_groups)
        return self

    def compute(self, **kwargs):
        self._result = dask.compute(self._joined_applied_groups, **kwargs)[0]
        return self

    def visualize_workflow(self, **kwargs):
        dask.visualize(self._joined_applied_groups, **kwargs)

    def get_result(self):
        return self._result

    def print_groups(self):
        pprint.pprint(self._groups, width=1)

    def get_groups(self):
        return copy.deepcopy(self._groups)

    @staticmethod
    def windowing(func):
        """

        This is a decorator that converts any function that accepts input data as a dataframe into a windowing function that will be applied to each chunks of the input data

        Arguments:
            func {object} -- function to be converted

        Returns:
            [dask.delayed] -- a windowing version of func
        """

        def wrapper_windowing(st, et, interval, step, load_func, chunk_func, join_func):
            # 骚操作
            start_windows = pd.date_range(
                start=st, end=et, freq=str(step) + 'S').values.tolist()
            stop_windows = start_windows + \
                pd.Timedelta(interval, unit='s').values.tolist()

            def wrapper_func(data, **kwargs):
                data = delayed(load_func)(data)
                chunk_results = []
                for start_window, stop_window in zip(start_windows, stop_windows):
                    chunk = delayed(chunk_func)(
                        data, start_window, stop_window)
                    chunk_result = delayed(func)(chunk, **kwargs)
                    chunk_results.append(chunk_result)
                return delayed(join_func)(chunk_results)
            return wrapper_func
        return wrapper_windowing

    def apply_by_window(self, func, st, et, interval, step, chunk_func, join_func, **kwargs):
        """
        Apply function to window chunks of each group's data input. Only
         applicable when data can be imported as dataframe

        Arguments:
            func {object} -- a function
            interval {float} -- [description]
            step {[float]} -- [description]
            ingroup_join {str} -- 'no': treat as independent; 'adjacent': combine adjacent objects (before and after) as one; 'all': combine all objects as one.
        """

        windowing_func = Groupby.windowing(func)(
            st, et, interval, step, chunk_func, join_func, **kwargs)
        self.apply(windowing_func, **kwargs)
        return self

    def apply(self, func, **kwargs):
        """[summary]

        Arguments:
            func {[type]} -- [description]
        """

        for group_name in self._groups:
            self._applied_groups[group_name] = [func(group_item, self._groups[group_name], **kwargs) for group_item in self._groups[group_name]]
        return self
