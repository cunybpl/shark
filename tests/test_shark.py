from shark.shark import SharkDataFrame
import pandas as pd
import numpy as np
from .fixtures import rtm_df
import unittest

tc = unittest.TestCase()

def test_init_and_validate_raise_error(rtm_df):
    rtm_df['datetime'] = rtm_df['datetime'].dt.strftime(date_format = '%B %d, %Y, %r')

    with tc.assertRaises(ValueError):
        rtm_df.shark.find_gaps('a', 'd')

def test_shark_hourly(rtm_df):

    gap_ls = rtm_df.shark.find_gaps('datetime', '15T')
    df_filled = rtm_df.shark.fill('datetime', variable_columns=['energy'], gaps_list=gap_ls)
    interpolate_df = df_filled.shark.interpolate('datetime', energy={})
    resample_df = interpolate_df.shark.resample('datetime', 'hourly', energy=np.mean)
    assert len(resample_df) == (8497 - 1)/4

def test_shark_daily(rtm_df):

    gap_ls = rtm_df.shark.find_gaps('datetime', '15T')
    df_filled = rtm_df.shark.fill('datetime', variable_columns=['energy'], gaps_list=gap_ls)
    interpolate_df = df_filled.shark.interpolate('datetime', energy={})
    resample_df = interpolate_df.shark.resample('datetime', 'daily', energy=np.mean)

    assert len(resample_df) == 30+31+27

def test_shark_weekly(rtm_df):

    gap_ls = rtm_df.shark.find_gaps('datetime', '15T')
    df_filled = rtm_df.shark.fill('datetime', variable_columns=['energy'], gaps_list=gap_ls)
    interpolate_df = df_filled.shark.interpolate('datetime', energy={})
    resample_df = interpolate_df.shark.resample('datetime', 'weekly', energy=np.mean)

    assert len(resample_df) == int(88/7)

def test_shark_monthly(rtm_df):

    gap_ls = rtm_df.shark.find_gaps('datetime', '15T')
    df_filled = rtm_df.shark.fill('datetime', variable_columns=['energy'], gaps_list=gap_ls)
    interpolate_df = df_filled.shark.interpolate('datetime', energy={})
    resample_df = interpolate_df.shark.resample('datetime', 'monthly', energy=np.mean)

    assert len(resample_df) == 2