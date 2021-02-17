import pandas as pd 
import numpy as np
from typing import List

class TSFill(object):

    def __init__(self, time_column, variable_columns: List):
        self.time_column = time_column
        self.variable_columns = variable_columns

    def _make_meta_data_row(self, df):
        df_copy = df.iloc[0:2,:].copy()
        column_ignored = self.variable_columns+[self.time_column]
        df_copy.drop(columns=column_ignored, inplace = True)

        default_row = df_copy.to_dict(orient='records')
        default_row = default_row[0] if default_row else {}
        default_dict = {**dict(zip(self.variable_columns, [np.nan]*len(self.variable_columns))),
                            **default_row}
        return default_dict
    
    def _make_final_frame(self, df, df_gaps):
        df = pd.concat([df, df_gaps], sort=True).sort_values(by=[self.time_column])
        df.reset_index(inplace = True)
        df.drop(columns='index', inplace = True)
        return df

    def _make_interval_gap_frame(self, df, new_ts):
        if len(new_ts) == 0:
            return pd.DataFrame()
        
        default_dict = self._make_meta_data_row(df)
        return pd.DataFrame({self.time_column: new_ts, **default_dict})

    def fill(self, df, gaps_list: List=[]):
        if not gaps_list:
            return df

        df_gaps = self._make_interval_gap_frame(df, gaps_list)
        df = self._make_final_frame(df, df_gaps)
        return df