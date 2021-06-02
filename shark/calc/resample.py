import pandas as pd
import numpy as np
from typing import List
from .utils import likely_freq_array, td_to_minutes

class TSResample(object):

    def __init__(self, time_column: str, timescale: str, metadata_cols: List=[]):
        self.time_column = time_column
        self.timescale = timescale
        self.metadata_cols = metadata_cols
        
    def _handle_metadata(self, df):
        if not self.metadata_cols:
            return {}
        return df[self.metadata_cols].iloc[0, :].to_dict()

    def _prep_args_timescale(self, timescale):
        """helper function for preparing timescale dependent arguments"""
        timescale_resample = {'hourly': '1H', 'daily': '1D', 'weekly': '1W', 'monthly': '1M'}
        freq_check = {'hourly': 60, 'daily': 60*24, 'weekly': 60*24*7, 'monthly': 60*24*30}
        return timescale_resample[timescale], freq_check[timescale] 

    def _add_metadata_to_resampled_df(self, df, metadata_dict):
        if not metadata_dict:
            return df
        for k, v in metadata_dict.items():
            df[k] = v
        return df

    def resample(self, df, irregular: bool = False, num_limit_points: int = 0, **kwargs):
        """resample method

        Args:
            df ([type]): dataframe
            irregular (bool, optional): a flag to indicate whether incoming timeseries data is irregular;
                                        i.e timescales are not in consistent interval by any means.
                                        If it is set to True, as long as  numbers of points in an interval is greater than
                                        or equal to num_limit_points,points in that interval will be resampled and included in the final result.
                                        Defaults to False.
            num_limit_points (int, optional): If irregular is set to true, num_limit_points should be provided and set to at least 1.
                                            This is to ensure that incomplete interval(for example hours) aren't included.
                                            Defaults to 0.

        Returns:
            [type]: [description]
        """        
        df = df.copy()
        
        if irregular:
            assert num_limit_points > 0, "irregular is set to True. Hence, num_limit_points > 0 must be provided."

        timescale_resample, freq_check_val = self._prep_args_timescale(self.timescale)
        freq = abs(int(td_to_minutes(likely_freq_array(df[self.time_column]))))

        metadata_dict = self._handle_metadata(df)

        df['count'] = 1 # make sure incomplete hours aren't included

        kwargs['count'] = np.sum

        df = df.resample(timescale_resample, on = self.time_column)\
            .agg(kwargs).reset_index()
        
        if self.timescale == 'monthly': #prepare freq_check_val for monthly since different month differnt number of days
            freq_check_val = 60*24*df[self.time_column].dt.daysinmonth
            df[self.time_column] = df[self.time_column].apply(lambda x: x.replace(day=1))

        if irregular:
             df = df[df['count'] >= num_limit_points].drop(['count'], axis = 1) # pick hours with at least num_limit_points observation
        else:
            complete_chunk = freq_check_val / freq # how many points is a complete hour?
            df = df[df['count'] == complete_chunk].drop(['count'], axis = 1) # drop incomplete hours/intervals

        return self._add_metadata_to_resampled_df(df, metadata_dict)