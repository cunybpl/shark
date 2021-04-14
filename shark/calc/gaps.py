import pandas as pd
import numpy as np 
from .utils import add_delta_time_col, likely_freq, td_to_minutes
from typing import List

class TSFindGaps(object):

    def __init__(self, time_column: str):
        self.time_column = time_column
        self.freq = ''

    def _detect_freq(self, df, freq: str):
        if not freq:
            df_temp = add_delta_time_col(df.copy(), self.time_column)
            _, freq = self._get_duration_freq(df_temp)
            freq = str(freq)+'T'
        return freq
    
    def set_freq(self, df, freq):
        self.freq = self._detect_freq(df, freq)
    
    def get_freq(self):
        return self.freq
    
    def _get_duration_freq(self, df):
        duration = likely_freq(df)
        freq = abs(td_to_minutes(duration))
        return duration, freq

    def remove_duplicates(self, df):
        return df.drop_duplicates(subset=self.time_column)
    
    def _get_gaps_in_ts(self, df, freq):
        og_ts = set(df[self.time_column].to_list())
        comp_ts = set(pd.date_range(min(og_ts), max(og_ts), freq=freq).to_list())
        return list(comp_ts - og_ts)
    

    def find_gaps(self, df, freq: str='') -> List:
        df = self.remove_duplicates(df.copy())
        self.set_freq(df, freq)
        new_ts = self._get_gaps_in_ts(df, self.freq)

        return new_ts
