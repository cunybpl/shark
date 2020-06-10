'''
Test for the RTM dataframe extension
'''
from shark.dataframe_ext import SharkDataFrame

import unittest
from . import FIXTURES_DIR
import pandas as pd
import numpy as np
import os
from pandas.testing import *

RTM_DF = pd.read_json(os.path.join(FIXTURES_DIR,'rtm_15m_w_gaps.json'), 
                        orient='records')


class TestSharkDataFrameExt(unittest.TestCase):

    def setUp(self):
        self.rtm_df = RTM_DF
        self.rtm_df['dateStamp'] = pd.to_datetime(self.rtm_df['dateStamp'])
        self.rtm_df['demand'] = self.rtm_df['demand'].astype(float)

        datetime = pd.date_range('2017-09-01 00:00:00', '2017-11-28 12:00:00', freq='15T').to_list()
        data =[{'datetime': t, 'energy': 2.0, 'demand':3.0} for t in datetime]
        [data.pop(i) for i in [10]*4]
        self.df = pd.DataFrame(data)

    def test_init_and_validate(self):
        # test that a proper df passes and that the function doesn't
        # alter the df
        temp2 = self.rtm_df.copy()
        temp_df = self.rtm_df.shark._validate(self.rtm_df)
        assert_frame_equal(temp_df, self.rtm_df)
        # checking purity
        assert_frame_equal(temp2,self.rtm_df)
        #test that extra cols are fine
        temp_df['blah'] = 1
        temp_df['bleh'] = False
        assert_frame_equal(temp_df, temp_df.shark._validate(temp_df))
        # test that datetime column validation works
        temp_df['dateStamp'] = temp_df['dateStamp'].dt.strftime(date_format = '%B %d, %Y, %r')
        self.assertRaises(
            ValueError,
            self.rtm_df.shark._validate, temp_df
        )
        
        # test a diff time scale
        temp_df = self.rtm_df.resample('1W', on = 'dateStamp')\
            .mean().reset_index()
        assert_frame_equal(
            temp_df,
            temp_df.shark._validate(temp_df)
        )

    def test_clean(self):
        self.assertEqual(len(self.df), 8493)
        temp = self.df.shark.clean({'energy': {}})
        self.assertEqual(len(temp), 8497)

        datetime = pd.date_range('2017-09-01 00:00:00', '2017-11-28 12:00:00', freq='15T').to_list()
        self.assertListEqual(datetime, temp['datetime'].to_list())

    def test_resample_agg_config(self):
        df = self.df.copy()
        df['stuff'] = 1
        default_res = df.shark._resample_agg_config({'energy' : np.sum}, {'energy': {}, 'demand':{'limit': 0}})
        expected_res = {'count' : np.sum, 'energy' : np.sum, 'demand': np.mean,
                        'energy_interpolated': np.mean,'stuff':np.mean}
        self.assertDictEqual(default_res, expected_res)

        custom_agg_dict = {'stuff': np.sum, 'demand': np.sum, 'energy' : np.sum}
        mod_res = df.shark._resample_agg_config(custom_agg_dict, {'energy': {}, 'demand':{'limit': 0}})
        expected_res['stuff'] = np.sum
        expected_res['demand'] = np.sum
        self.assertDictEqual(mod_res, expected_res)

    def test_prep_hour_of_week_X_y(self):
        x,y = self.df.shark.prep_hour_of_week_X_y({'energy': {}})
        self.assertEqual((8497 - 1)/4, len(x)) #8497 is the length after filling, -1 is for the last incomplete hour
        self.assertEqual((8497 - 1)/4, len(y))

    def test_prep_hour_of_day_X_y(self):
        x,y = self.df.shark.prep_hour_of_day_X_y({'energy': {}})
        self.assertEqual((8497 - 1)/4, len(x)) #8497 is the length after filling, -1 is for the last incomplete hour
        self.assertEqual((8497 - 1)/4, len(y))

    def test_prep_day_of_week_X_y(self):
        x,y = self.df.shark.prep_day_of_week_X_y({'energy': {}})
        self.assertEqual(30+31+27,len(x))
        self.assertEqual(30+31+27,len(y))

    def test_prep_weekly_X_y(self):
        x,y = self.df.shark.prep_weekly_X_y({'energy': {}})
        self.assertEqual(int(88/7),len(x))
        self.assertEqual(int(88/7),len(y))

    def test_prep_monthly_X_y(self):
        x,y = self.df.shark.prep_monthly_X_y({'energy': {}})
        self.assertEqual(2,len(x))
        self.assertEqual(2,len(y))
    
    def test_prep_timescale_X_y_value_error(self):
        datetime = pd.date_range('2017-09-01 00:00:00', '2017-11-28 12:00:00', freq='61T').to_list()
        data =[{'datetime': t, 'energy': 2.0, 'demand':3.0} for t in datetime]
        df = pd.DataFrame(data)

        with self.assertRaises(ValueError):
            df.shark.prep_hour_of_day_X_y({'energy': {}})
        
        with self.assertRaises(ValueError):
            df.shark.prep_hour_of_week_X_y({'energy': {}})
        
        datetime = pd.date_range('2017-09-01 00:00:00', '2017-11-28 12:00:00', freq='14401T').to_list()
        data =[{'datetime': t, 'energy': 2.0, 'demand':3.0} for t in datetime]
        df = pd.DataFrame(data)

        with self.assertRaises(ValueError):
            df.shark.prep_day_of_week_X_y({'energy': {}})

        datetime = pd.date_range('2017-09-01 00:00:00', '2017-11-28 12:00:00', freq='10081T').to_list()
        data =[{'datetime': t, 'energy': 2.0, 'demand':3.0} for t in datetime]
        df = pd.DataFrame(data)

        with self.assertRaises(ValueError):
            df.shark.prep_weekly_X_y({'energy': {}})
        
        datetime = pd.date_range('2017-09-01 00:00:00', '2018-01-28 12:00:00', freq='46081T').to_list()
        data =[{'datetime': t, 'energy': 2.0, 'demand':3.0} for t in datetime]
        df = pd.DataFrame(data)

        with self.assertRaises(ValueError):
            df.shark.prep_monthly_X_y({'energy': {}})

    def test_exclude_col_from_X(self):
        df = self.df.copy()
        df['stuff'] = 1
        #24 cols + len([energy, energy_interpolated, datetime, stuff]) = 28
        x, y = df.shark.prep_hour_of_day_X_y({'energy': {}, 'demand':{'limit': 0}})
        self.assertEqual(28, len(x[0]))

        x, y = df.shark.prep_hour_of_day_X_y({'energy': {}, 'demand':{'limit': 0}}, exclude_from_X=['datetime', 'stuff', 'energy_interpolated'])
        self.assertEqual(25, len(x[0]))

