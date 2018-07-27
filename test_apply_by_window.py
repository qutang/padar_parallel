import pandas as pd
from dask import delayed
import dask
import numpy as np
from padar_parallel.groupby import GroupBy
from padar_parallel.windowing import MhealthWindowing
from padar_converter.mhealth import dataframe, fileio


@delayed
@MhealthWindowing.groupby_windowing('sensor')
def sampling_rate(df):
    if df.empty:
        return pd.DataFrame()
    return pd.DataFrame({'NUMBER_OF_SAMPLES': [df.shape[0]]})


def load_data(data, all_data, **kwargs):
    metas = GroupBy.get_meta(data)
    return GroupBy.bundle(delayed(fileio.load_sensor)(GroupBy.get_data(data)), **metas)


@delayed
def as_dataframe(groups):
    group_dfs = []
    groups = GroupBy.get_data_groups(groups)
    for group_name in groups:
        group_df = pd.concat(groups[group_name])
        group_df['GROUP_NAME'] = group_name
        group_dfs.append(group_df)
    result = pd.concat(group_dfs)
    return result

if __name__ == '__main__':
    import pprint
    from glob import glob
    from padar_parallel.groupby import GroupBy
    from padar_parallel.grouper import MHealthGrouper
    from padar_converter.mhealth import dataset
    input_files = glob(
        'D:/data/spades_lab/SPADES_[1-9]/MasterSynced/**/Actigraph*.sensor.csv', recursive=True)
    pprint.pprint(input_files)
    grouper = MHealthGrouper(input_files)
    groupby_obj = GroupBy(
        input_files, **MhealthWindowing.make_metas(input_files))
    groupby_obj.split(grouper.pid_group(),
                      grouper.sid_group(),
                      ingroup_sortkey_func=lambda x: dataset.get_file_timestamp(GroupBy.get_data(x)))
    groupby_obj.apply(load_data)
    groupby_obj.apply(sampling_rate, interval=12.8, step=12.8) \
        .final_join(join_func=as_dataframe)

    groupby_obj.visualize_workflow(filename='test_apply_by_window.pdf')
    result = groupby_obj.compute(
        scheduler='processes').show_profiling().get_result()
    result.to_csv('test.csv', index=True)
