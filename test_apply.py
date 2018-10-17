import pandas as pd
from dask import delayed
import dask
import numpy as np
from padar_parallel.groupby import GroupBy
from padar_parallel.join import join_as_dataframe


def count_total_rows(data, all_data, **kwargs):

    @delayed
    def load_data(data):
        return pd.read_csv(data, parse_dates=[0], infer_datetime_format=True)

    @delayed
    def count(data):
        return data.shape[0]

    df = load_data(GroupBy.get_data(data))
    return GroupBy.bundle(count(df))


@delayed
def greater_than_zero(data, all_data):
    return GroupBy.bundle(GroupBy.get_data(data) > 0)


@delayed
def sum_rows(group_items, **kwargs):
    group_items = [GroupBy.get_data(group_item) for group_item in group_items]
    return GroupBy.bundle(np.sum(group_items))


@delayed
def as_dataframe(groups, group_types):
    groups = GroupBy.get_data_groups(groups)
    result = pd.DataFrame(groups)
    result = result.transpose()
    return result


if __name__ == '__main__':
    import pprint
    from glob import glob

    from padar_parallel.grouper import MHealthGrouper
    from padar_converter.mhealth import dataset
    input_files = glob(
        'D:/data/spades_lab/SPADES_[1-2]/MasterSynced/**/Actigraph*.sensor.csv', recursive=True)
    pprint.pprint(input_files)
    grouper = MHealthGrouper(input_files)
    groupby_obj = GroupBy(input_files) \
        .split(grouper.pid_group(),
               grouper.sid_group(),
               group_types=['PID', 'SID'],
               ingroup_sortkey_func=lambda x: dataset.get_file_timestamp(x['data']))

    groupby_obj.apply(count_total_rows) \
        .post_join(join_func=sum_rows) \
        .final_join(join_func=as_dataframe)

    groupby_obj.visualize_workflow(filename='test_apply.pdf')
    result = groupby_obj.compute(scheduler='processes').get_result()
    print(result)
