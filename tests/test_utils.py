import unittest
import numpy as np
from pprint import pprint
from shark.utils import *

import pandas as pd


class TestUtilFunctions(unittest.TestCase):

    def setUp(self):
        pass


    def test_custom_resampler(self):
        # TODO: test with shuffle as well to ensure order makes no difference!

        # Make a Fake time series where all the values are 1 with time stamps every 1 hr
        fake_df = pd.DataFrame()
        fake_df['interval_start'] = pd.to_datetime(pd.date_range(start = "2000/01/01", freq = '1H', periods = 24 * 31))
        fake_df['value'] = 1
        # Drop some random values to create fake gaps and check interpolation
        fake_df = fake_df.drop([2, 4, 24, 25, 42, 420], axis = 0)
        period_end_time = pd.to_datetime(["2000-01-01 03:00:00", 
                                        "2000-01-05 00:00:00", 
                                        "2000-01-10 00:00:00", 
                                        "2000-01-15 00:00:00"])
        period_start_time = pd.to_datetime(["1999-12-20 02:00:00", 
                                            "2000-01-01 03:00:00",
                                            "2000-01-05 00:00:00", 
                                            "2000-01-10 00:00:00"])

        expected = [(5 - 1) * 24 - 3, 24 * 5, 24 * 5]
        self.assertListEqual(list(custom_resampler(fake_df, 
                                                    'interval_start', 
                                                    period_end_time, 
                                                    period_start_time).sum().values),
                            expected)
        fake_df = pd.DataFrame()
        fake_df['interval_start'] = pd.to_datetime(pd.date_range(start = "2000/01/01", freq = '1H', periods = 10))
        fake_df['value'] = np.arange(1, len(fake_df)+ 1)
        # 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
        fake_df = fake_df.drop([2, 4, 5], axis = 0) 
        # 1, 2, _, 4, _, _, 7, 8, 9, 10
        period_end_time = pd.to_datetime(["2000-01-01 03:00:00", 
                                         "2000-01-01 05:00:00",
                                         "2000-01-01 09:00:00"])
        period_start_time = pd.to_datetime(["2000-01-01 00:00:00", 
                                         "2000-01-01 03:00:00",
                                         "2000-01-01 05:00:00"])

        expected = [1 + 2 + 3, 4 + 5, 6 + 7 + 8 + 9]

        self.assertListEqual(list(custom_resampler(fake_df, 
                                                    'interval_start', 
                                                    period_end_time, 
                                                    period_start_time).sum().values),
                                expected)
        period_start_time = pd.to_datetime(["1999-12-01 00:00:00", 
                                         "2000-01-01 03:00:00",
                                         "2000-01-01 05:00:00"])
        self.assertListEqual(list(custom_resampler(fake_df, 
                                                    'interval_start', 
                                                    period_end_time, 
                                                    period_start_time).sum().values),
                                expected[1:])


        # Without gaps should behave just like normal resample when given proper
        # period start and end times
        fake_df = pd.DataFrame()
        fake_df['interval_start'] = pd.to_datetime(pd.date_range(start = "2001/01/01", 
                                                                    freq = '1D', 
                                                                    periods = 365))
        fake_df['value'] = 1
        period_end_time = pd.to_datetime(pd.date_range(start = "2001/02/01", 
                                                                    freq = '1M', 
                                                                    periods = 12)).map(lambda dt: dt.replace(day=1))
        period_start_time = pd.to_datetime(pd.date_range(start = "2001/01/01", 
                                                                    freq = '1M', 
                                                                    periods = 12)).map(lambda dt: dt.replace(day=1))
        self.assertListEqual(list(custom_resampler(fake_df, 
                                                    'interval_start', 
                                                    period_end_time, 
                                                    period_start_time).sum().values),
                            list(fake_df.resample('1m', on = "interval_start").sum().values))
        # if we introduce gaps though it should be diff
        fake_df2 = fake_df.drop([2, 4, 24, 25, 42], axis = 0)
        self.assertFalse(
            list(fake_df2.resample('1m', on = "interval_start").sum().values) ==
            list(custom_resampler(fake_df2, 
                                    'interval_start', 
                                    period_end_time, 
                                    period_start_time).sum().values)
        )

        self.assertListEqual(list(custom_resampler(fake_df, 
                                                    'interval_start', 
                                                    period_end_time, 
                                                    period_start_time).sum().values),
                            list(custom_resampler(fake_df2, 
                                                    'interval_start', 
                                                    period_end_time, 
                                                    period_start_time).sum().values))

        
