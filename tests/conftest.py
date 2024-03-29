import pytest
import pandas as pd
import json
from . import RTM_DATAPATH

@pytest.fixture
def rtm_data_with_gaps():
    with open(RTM_DATAPATH, 'r') as f:
        return json.load(f)

@pytest.fixture
def datetime_vec():
    datetime_ = pd.date_range('2017-09-01 00:00:00', '2017-09-02 00:00:00', freq='15T').to_list()
    return datetime_

@pytest.fixture
def rtm_df():
    with open(RTM_DATAPATH, 'r') as f:
        data = json.load(f)
    rtm_df = pd.DataFrame(data)
    rtm_df['dateStamp'] = pd.to_datetime(rtm_df['dateStamp'])
    rtm_df['demand'] = rtm_df['demand'].astype(float)

    datetime = pd.date_range('2017-09-01 00:00:00', '2017-11-28 12:00:00', freq='15T').to_list()
    data =[{'datetime': t, 'energy': 2.0, 'demand':3.0} for t in datetime]
    [data.pop(i) for i in [10]*4]
    df = pd.DataFrame(data)

    return df

@pytest.fixture
def irregular_data():
    datetime_ls = ['2017-09-01 00:00:00',
                '2017-09-01 00:51:00', 
                '2017-09-01 01:21:00', 
                '2017-09-01 01:36:00', 
                '2017-09-01 01:51:00', 
                '2017-09-01 02:51:00', 
                '2017-09-01 03:11:00', 
                '2017-09-01 03:51:00']

    data = [ {'datetime':i , 'a': 1} for i in datetime_ls]
    df = pd.DataFrame(data)
    df['datetime'] = pd.to_datetime(df['datetime'])

    return df
    

