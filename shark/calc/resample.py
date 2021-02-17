import pandas as pd
import numpy as np
from typing import List
from .utils import likely_freq_array, td_to_minutes

class TSResample(object):

    def __init__(self, time_column: str, timescale: str):
        self.time_column = time_column
        self.timescale = timescale

    def _prep_args_timescale(self, timescale):
        """helper function for preparing timescale dependent arguments"""
        timescale_resample = {'hourly': '1H', 'daily': '1D', 'weekly': '1W', 'monthly': '1M'}
        freq_check = {'hourly': 60, 'daily': 60*24, 'weekly': 60*24*7, 'monthly': 60*24*30}
        return timescale_resample[timescale], freq_check[timescale] 

    def _resample_agg_config(self, cols: List, **kwargs):
        kwargs['count'] = np.sum
        other_cols = set(cols) - set(list(kwargs.keys()) + [self.time_column, 'count'])
        if not len(other_cols):
            return kwargs 
        for col in other_cols:
            kwargs[col] = np.mean
        return kwargs

    def resample(self, df, **kwargs):
        df = df.copy()

        timescale_resample, freq_check_val = self._prep_args_timescale(self.timescale)
        freq = abs(int(td_to_minutes(likely_freq_array(df[self.time_column]))))
        columns = df.columns.to_list()

        df['count'] = 1 # make sure incomplete hours aren't included

        df = df.resample(timescale_resample, on = self.time_column)\
            .agg(self._resample_agg_config(cols=columns, **kwargs)).reset_index()
        
        if self.timescale == 'monthly': #prepare freq_check_val for monthly since different month differnt number of days
            freq_check_val = 60*24*df[self.time_column].dt.daysinmonth
            df[self.time_column] = df[self.time_column].apply(lambda x: x.replace(day=1))

        complete_hour = freq_check_val / freq # how many points is a complete hour?
        df = df[df['count'] == complete_hour].drop(['count'], axis = 1) # drop incomplete hours
        return df