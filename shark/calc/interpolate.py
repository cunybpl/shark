import pandas as pd
import numpy as np

class TSInterpolate(object):

    def __init__(self, time_column: str, **kwargs):
        """Interpolate a dataframe with timeseries index.
        """ 
        assert kwargs, "kwargs must be provided."
        self.time_column = time_column
        self.config = kwargs
    
    def _add_interpolated_flag_cols(self, df, k):
        interpolated_flag_col = k + '_interpolated'
        if not interpolated_flag_col in df.columns: #init if no flag col
            df[k+"_interpolated"] = np.isnan(df[k]).astype(int)
        else:#adjust if flag col
            df[k+"_interpolated"] = (df[k+"_interpolated"] + np.isnan(df[k]).astype(int))%2
        return df
   
    def interpolate(self, df, add_flag: bool=True):
        df = df.copy()
        df.sort_values(by=[self.time_column], inplace=True)

        for k, v in self.config.items():
            df = self._add_interpolated_flag_cols(df, k) if add_flag else df
            df[k] = df[k].interpolate(**v)
            df = self._add_interpolated_flag_cols(df, k) if add_flag else df
        return df