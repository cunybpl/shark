from shark.preprocessing.encoding import *

import unittest
import pandas as pd
from pandas.testing import *


class TestEncoder(unittest.TestCase):
    def setUp(self):
        self.df_daily = pd.DataFrame()
        self.df_daily['datetime'] = pd.date_range("2020-01-01", "2020-01-10")
        self.df_daily['datetime'] = pd.to_datetime(self.df_daily['datetime'])
        self.df_daily['some_var'] =  np.random.rand(len(self.df_daily))

        self.df_one_day = pd.DataFrame()
        self.df_one_day['datetime'] = pd.date_range("2020-01-01", "2020-01-02", 
                                                    freq = '1H')[:-1]
        self.df_one_day['datetime'] = pd.to_datetime(self.df_one_day['datetime'])
        self.df_one_day['some_var'] = np.random.rand(len(self.df_one_day))

        self.df_one_week = pd.DataFrame()
        self.df_one_week['datetime'] = pd.date_range(
            "2020-01-06", "2020-01-13", freq = '1H')[:-1]
        self.df_one_week['datetime'] = pd.to_datetime(self.df_one_week['datetime'])
        self.df_one_week['some_var'] = np.random.rand(len(self.df_one_week))

    def test_holidays(self):
        # test pure
        temp = self.df_daily.copy(deep = True)
        result = self.df_daily.pipe(encode_holiday, "datetime")
        assert_frame_equal(temp, self.df_daily)
        # test idempotent
        temp = result.copy(deep = True)
        assert_frame_equal(
            temp,
            result.pipe(encode_holiday, "datetime")
        )
        # test that aside from day_off the result is the same as input
        assert_frame_equal(
            self.df_daily,
            self.df_daily.pipe(encode_holiday, "datetime")\
                .drop(['day_off'], axis = 1)
        )
        # test that Jan 1st was a holiday and everything else wasnt
        self.assertListEqual(
            [0 if c != 0 else 1 for c in range(0,10)],
            list(self.df_daily.pipe(encode_holiday, "datetime")['day_off'])
            )     

    def test_encode_hour_of_day(self):
        # test pure
        temp = self.df_one_day.copy(deep = True)
        result = self.df_one_day.pipe(encode_hour_of_day, "datetime")
        assert_frame_equal(
            temp,
            self.df_one_day
        )
        # test idempotent
        assert_frame_equal(
            result,
            result.pipe(encode_hour_of_day, "datetime")
        )
        # make sure it only affects new cols of df
        assert_frame_equal(
            temp,
            result[[c for c in result.columns if "hour_of_day" not in c]]
        )
        # make sure it does encode
        expected_result = pd.DataFrame()
        for i in range(1, 24):
            expected_result["hour_of_day_" + str(i)] = \
                np.array(
                    [0 if x != i else 1 for x in range(1,25)],
                    dtype = "uint8")
        assert_frame_equal(
            expected_result,
            result.drop(['datetime', 'some_var'], axis = 1)
        )

    def test_encode_hour_of_week(self):
        # test pure
        temp = self.df_one_week.copy(deep = True)
        result = self.df_one_week.pipe(encode_hour_of_week, "datetime")
        assert_frame_equal(
            temp,
            self.df_one_week
        )
        # test idempotent
        assert_frame_equal(
            result,
            result.pipe(encode_hour_of_week, "datetime")
        )
        # make sure it only affects new cols of df        
        assert_frame_equal(
            temp,
            result[[c for c in result.columns if "hour_of_week" not in c]]
        )
        # make sure it does indeed encode
        expected_result = pd.DataFrame()
        for i in range(1, 168):
            expected_result["hour_of_week_" + str(i)] = \
                np.array(
                    [0 if x != i else 1 for x in range(1,len(self.df_one_week) + 1)],
                    dtype = "uint8")
        assert_frame_equal(
            expected_result,
            result.drop(['datetime', 'some_var'], axis = 1)
        )



    def test_encode_day_of_week(self):
        # make sure pure
        temp = self.df_one_week.copy(deep = True)
        print(self.df_one_week.columns.to_list())
        result = self.df_one_week.pipe(encode_day_of_week, "datetime")
        assert_frame_equal(
            temp,
            self.df_one_week
        )
        # make sure idempotent
        # assert_frame_equal(
        #     result,
        #     result.pipe(encode_day_of_week, "datetime")
        # )
        # make sure it encodes as intended!
        expected_result = pd.DataFrame()
        cols = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 
            'Friday', 'Saturday']

        def temp_func(col_indx):
            temp_list = []
            for i in range(0,7):
                if i == col_indx:
                    temp_list += [1] * 24
                else:
                    temp_list += [0] * 24
            return np.array(temp_list, dtype = "uint8")

        for col_indx in range(len(cols)):
            expected_result[cols[col_indx]] = temp_func(col_indx)
        assert_frame_equal(
            expected_result.sort_index(axis=1),
            result.drop(['datetime', 'some_var'], axis = 1).sort_index(axis=1)
        )