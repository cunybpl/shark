import pandas as pd
import numpy as np
from datetime import datetime
import operator
import logging

l = logging.getLogger(__name__)

#minute for now, we will have to sketch abstract classes and time scale family class later.
#so here is the strategy:
#a class that only takes list of records of interval data and fill the gap and interpolate
#then classes that aggregate things <- now this depends on timescale

def add_delta_time_col(df, time_column):
    df['deltaTime'] = (df[time_column] - df[time_column].shift(-1))
    return df

def likely_freq(df):
    distribution_time_deltas = dict(df['deltaTime'].value_counts())
    ret = max(distribution_time_deltas.items(), key=operator.itemgetter(1))[0]
    return ret

def td_to_minutes(duration):
    return float(duration.seconds/(60) + duration.days*24*60)

def pop_key_with_limit_zero(config_object):
    config_object = config_object.copy()
    pop_key = []
    for k,v in config_object.items():
        if 'limit' in v.keys() and v['limit'] == 0:
            pop_key.append(k)
    for i in pop_key:
        config_object.pop(i)
    return config_object
    
class TSInterpolate:
    """
    config = {'demand': {}, 'energy': {'limit': 0}}
    #if limit is set to 0, that variable column will not be interpolated.
    #and if the dict is empty dict, default value for pd.interpolate will be used.

    inter = TSInterpolate(time_column='date', interpolate_config=config)
    #where data is a list of records and 'date' is the time column and 'energy' and 'demand' are variable columns to be interploated
    df_no_gaps = inter.fill(df_with_gaps)
    final_df = inter.interpolate(df_no_gaps)
    """
    def __init__(self, time_column, interpolate_config):
        self.time_column = time_column
        self.variable_columns = list(interpolate_config.keys())
        self.config = pop_key_with_limit_zero(interpolate_config)
    
    def _get_gaps_df(self, df, freq):
        og_ts = set(df[self.time_column].to_list())
        comp_ts = set(pd.date_range(min(og_ts), max(og_ts), freq=freq).to_list())
        return list(comp_ts - og_ts)
    
    def _make_meta_data_row(self, df):
        df_copy = df.iloc[0:2,:].copy()
        column_ignored = self.variable_columns+[self.time_column]
        df_copy.drop(columns=column_ignored, inplace = True)

        default_row = df_copy.to_dict(orient='records')
        default_row = default_row[0] if default_row else {}
        default_dict = {**dict(zip(self.variable_columns, [np.nan]*len(self.variable_columns))),
                            **default_row}
        return default_dict


    def _make_interval_gap_frame(self, df, new_ts):
        if len(new_ts) == 0:
            return pd.DataFrame()
        
        default_dict = self._make_meta_data_row(df)
        return pd.DataFrame({self.time_column: new_ts, **default_dict})
    
    def _make_final_frame(self, df, df_gaps):
        df = pd.concat([df, df_gaps], sort=True).sort_values(by=[self.time_column])
        df.reset_index(inplace = True)
        df.drop(columns='index', inplace = True)
        return df
    
    def _get_duration_freq(self, df):
        duration = likely_freq(df)
        freq = abs(td_to_minutes(duration))
        return duration, freq
    
    #this will be somewhat of an over kill since df.drop_duplicates(time_column) is simple enough
    def remove_duplicates(self, df):
        return df.drop_duplicates(subset=self.time_column)
    
    def interpolate(self, df):
        df = df.copy()
        for k, v in self.config.items():
            df = self._add_interpolated_flag_cols(df, k)
            df[k] = df[k].interpolate(**v)
            df = self._add_interpolated_flag_cols(df, k)
        return df
    
    def _add_interpolated_flag_cols(self, df, k):
        interpolated_flag_col = k + '_interpolated'
        if not interpolated_flag_col in df.columns: #init if no flag col
            df[k+"_interpolated"] = np.isnan(df[k]).astype(int)
        else:#adjust if flag col
            df[k+"_interpolated"] = (df[k+"_interpolated"] + np.isnan(df[k]).astype(int))%2
        return df
    
    def fill(self, df):
        """Carpe lacunam! ('Sieze THE GAP!'): https://www.youtube.com/watch?v=Rt4rzRCy_XU
        """
        df = self.remove_duplicates(df.copy())
        df_temp = add_delta_time_col(df.copy(), self.time_column)
        _, freq = self._get_duration_freq(df_temp)
        new_ts = self._get_gaps_df(df, str(freq)+'T')
        l.info(new_ts)
        df_gaps = self._make_interval_gap_frame(df, new_ts)
        l.info(df_gaps)
        df = self._make_final_frame(df, df_gaps)
        return df


#this will be somewhat of an over kill since df.drop_duplicates(time_column) is simple enough
def remove_duplicates(df, time_column, config):
    inter = TSInterpolate(time_column, config)
    return inter.remove_duplicates(df)


def fill(df, time_column, config = {}):
    inter = TSInterpolate(time_column, config)
    return inter.fill(df)

#validation if time column is datetime
def interpolate(df, time_column, config = {}):
    inter = TSInterpolate(time_column, config)
    return inter.interpolate(df)

#custom check for datetime column
def has_datetime_columns(df):
    time_len = len(df.select_dtypes(include=['datetime64[ns]']))
    if not time_len:
        raise AssertionError("No datetimecolumn found. DataFrame must contains a datetime column.")
    return df