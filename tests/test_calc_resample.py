from .fixtures import datetime_vec
from shark.calc import TSResample 
import pandas as pd 
import numpy as np

def test_resample(datetime_vec):
    #the last hour is incomplete, only one point in the next day
    data =[{'datetime': t, 'a': 2, 'b':3} for t in datetime_vec]
    df_hourly = pd.DataFrame(data)

    resampling = TSResample(time_column='datetime', timescale="hourly")
    exp_df = resampling.resample(df_hourly, a = np.sum, b=np.mean)
    records = exp_df.to_dict(orient="records")
    assert set(['datetime', 'a', 'b']) == set(exp_df.columns.to_list())
    assert (len(data)-1)/4 == len(records)

def test_resample_with_metadata(datetime_vec):
    #the last hour is incomplete, only one point in the next day
    data =[{'datetime': t, 'a': 2, 'b':3, 'c': 'id', 'd': 'boo'} for t in datetime_vec]
    df_hourly = pd.DataFrame(data)

    resampling = TSResample(time_column='datetime', timescale="hourly", metadata_cols=['c', 'd'])
    exp_df = resampling.resample(df_hourly, a = np.sum, b=np.mean)
    records = exp_df.to_dict(orient="records")

    assert set(['datetime', 'a', 'b', 'c', 'd']) == set(exp_df.columns.to_list())
    assert (len(data)-1)/4 == len(records)
    assert records[0]['c'] == 'id'
    assert records[0]['d'] == 'boo'


    data =[{'datetime': t, 'a': 2, 'b':3, 'c': 'id', 'd': 'boo', 'e': 5} for t in datetime_vec]
    df_hourly = pd.DataFrame(data)

    resampling = TSResample(time_column='datetime', timescale="hourly")
    exp_df = resampling.resample(df_hourly, a = np.sum, b=np.mean)
    records = exp_df.to_dict(orient="records")

    assert set(['datetime', 'a', 'b']) == set(exp_df.columns.to_list())
    assert 'e' not in exp_df.columns

    assert (len(data)-1)/4 == len(records)