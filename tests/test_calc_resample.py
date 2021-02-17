from .fixtures import datetime_vec
from shark.calc import TSResample 
import pandas as pd 
import numpy as np

def test_resample(datetime_vec):
    #the last hour is incomplete, only one point in the next day
    data =[{'datetime': t, 'a': 2, 'b':3, 'c': 4} for t in datetime_vec]
    df_hourly = pd.DataFrame(data)

    resampling = TSResample(time_column='datetime', timescale="hourly")
    exp_df = resampling.resample(df_hourly, a = np.sum, b=np.mean)
    records = exp_df.to_dict(orient="records")
    assert (len(data)-1)/4 == len(records)

def test_resample_agg_config():
    resampling = TSResample(time_column='datetime', timescale="hourly")
    kwargs = {'a': np.sum}
    exp_dict = {'a': np.sum, 'b': np.mean, 'c': np.mean}
    res_dict = resampling._resample_agg_config(['datetime', 'a', 'b', 'c'], **kwargs)

    for k,v in exp_dict.items():
        assert res_dict[k] == v