import pandas as pd
from dask import delayed
import dask
import numpy as np

def count_rows(data):

    @delayed
    def load_data(data):
        return pd.read_csv(data, parse_dates=[0], infer_datetime_format=True)

    @delayed
    def concat_data(data):
        return pd.concat(data, axis=0)

    @delayed
    def count(data):
        return data.shape[0]
    
    dfs = [load_data(d) for d in data]
    df = concat_data(dfs)
    return count(df)

@delayed
def post_join_func(group):
    return [np.sum(group)]

@delayed
def final_join_func(groups):
    return pd.DataFrame(groups)

if __name__ == '__main__':
    import pprint
    from glob import glob
    from padar_parallel.groupby import GroupBy
    from padar_parallel.grouper import MHealthGrouper
    from padar_converter.mhealth import dataset
    input_files = glob(
        'D:/data/spades_lab/SPADES_[1-2]/MasterSynced/**/Actigraph*.sensor.csv', recursive=True)
    pprint.pprint(input_files)
    grouper = MHealthGrouper(input_files)
    groupby_obj = GroupBy(*input_files) \
        .split(grouper.pid_group(),
               grouper.sid_group(), ingroup_sortkey_func=dataset.get_file_timestamp) \
        .pre_join(join_func=GroupBy.join_nothing) \
        .apply(count_rows) \
        .post_join(join_func=post_join_func) \
        .final_join(join_func=final_join_func)
    groupby_obj.visualize_workflow(filename='test.pdf')
    result = groupby_obj.compute(scheduler='processes').get_result()
    # print(result)
    # groupby_obj.print_groups()
