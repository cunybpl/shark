""" extension namespaces for pandas. Adds extra utilities to DataFrame objects when this library is in use.
"""
from pandas.api.extensions import register_dataframe_accessor
from shark.preprocessing.tsinterpolation import fill, interpolate, pop_key_with_limit_zero
from shark.preprocessing.extract_xy import Xmat_y 
from shark.preprocessing.encoding import encode_day_of_week, encode_holiday, encode_hour_of_day, encode_hour_of_week
import numpy as np 
import pandas as pd
from shark.utils import likely_freq, td_to_minutes

# ​from shark.preprocessing import encoding, extract_xy, tsinterpolation


class DataFrameAccessor(object):
    """ A base class with default methods for all extension classes
    """

    def __init__(self, df):
        self._df = self._validate(df)


    def _validate(self, df):
        return df  


    def clean(self):
        return self._df



@register_dataframe_accessor('shark')
class SharkDataFrame(DataFrameAccessor):
    """ different combinations of things. 

    These pull your data in y, X feature matrices for sklearn and wrap the cleaning methods in there. 


    """
    def _validate(self, df):
        '''
        Ensures the DF has a time column.
        '''
        
        if len(df.select_dtypes(include = ['datetime64']).columns) == 1:
            self._datetime_col = df.select_dtypes(include = ['datetime64']).columns[0]
        else:
            raise ValueError("No datetime column in df")
        
        return df.copy(deep = True)
    

    def clean(self, interpolate_config = {}):
        '''
        Fixes small issues with data such as duplicated values,
        nans in datetime, ensuring every point has lag of 1m
        and giving an option to interpolate.
        '''
        return self._df.pipe(fill, self._datetime_col, interpolate_config)\
                        .pipe(interpolate, self._datetime_col, interpolate_config)

    def interpolate(self, interpolate_config= {}):
        return self._df.pipe(interpolate, self._datetime_col, interpolate_config)
    
    def fill(self, interpolate_config = {}):
        return self._df.pipe(fill, self._datetime_col, interpolate_config)

    def _resample_agg_config(self, custom_agg_dict, interpolate_config): # TODO : TEST!
        # those need to be sum
        default_config_dict = {'count' : np.sum}
        # default of the rest is mean
        for col in list(set(self._df.columns) - set([self._datetime_col, 'count'])):
            default_config_dict[col] = np.mean
        
        for k,v in custom_agg_dict.items(): # but those can be overwritten by custom arg
            if (k in self._df.columns) & (k != self._datetime_col):# disallow nonsense
                default_config_dict[k] = v
        return self._add_interpolated_flag_column(default_config_dict, interpolate_config)
    
    def _add_interpolated_flag_column(self, resample_agg_kwarg, interpolate_config):
        """add interpolated_flag column to resample config.
        NOTE: Tanya's _resample_agg_config was initially handling only columns from original data frame
        and not including the cleaned one, which has interpolated flag columns; hence, this function..
        """
        out_dict = resample_agg_kwarg.copy()
        inter_key = set(pop_key_with_limit_zero(interpolate_config).keys())
        temp_dict = {}
        for key in inter_key.intersection(out_dict.keys()):
            temp_dict[key+'_interpolated'] = np.mean #between 1 and 0
        return {**out_dict, **temp_dict}

    def prep_hour_of_week_X_y(self, interpolate_config={'energy': {}, 'demand':{'limit': 0}}, resample_agg_kwargs={'energy': np.sum}, y_cols = ['energy'],exclude_from_X=[], skip_clean=False, skip_resample = False):
        return self._prep_timescale_X_y(interpolate_config, resample_agg_kwargs, 'hourly', 'hour of week', y_cols, exclude_from_X, skip_clean, skip_resample)

    def prep_hour_of_day_X_y(self, interpolate_config={'energy': {}, 'demand':{'limit': 0}}, resample_agg_kwargs={'energy': np.sum}, y_cols = ['energy'],exclude_from_X=[], skip_clean=False, skip_resample = False):
        return self._prep_timescale_X_y(interpolate_config, resample_agg_kwargs, 'hourly', 'hour of day', y_cols, exclude_from_X, skip_clean, skip_resample)
        
    def prep_day_of_week_X_y(self, interpolate_config={'energy': {}, 'demand':{'limit': 0}}, resample_agg_kwargs={'energy': np.sum}, y_cols = ['energy'],exclude_from_X=[], skip_clean=False, skip_resample = False):
        return self._prep_timescale_X_y(interpolate_config, resample_agg_kwargs, 'daily', 'day of week', y_cols, exclude_from_X, skip_clean, skip_resample)

    def prep_weekly_X_y(self, interpolate_config={'energy': {}, 'demand':{'limit': 0}}, resample_agg_kwargs={'energy': np.sum}, y_cols = ['energy'],exclude_from_X=[], skip_clean=False, skip_resample = False):
        return self._prep_timescale_X_y(interpolate_config, resample_agg_kwargs, 'weekly', None, y_cols, exclude_from_X, skip_clean, skip_resample)

    def prep_monthly_X_y(self, interpolate_config={'energy': {}, 'demand':{'limit': 0}}, resample_agg_kwargs={'energy': np.sum},y_cols = ['energy'], exclude_from_X=[], skip_clean=False, skip_resample = False):
        return self._prep_timescale_X_y(interpolate_config, resample_agg_kwargs, 'monthly', None, y_cols, exclude_from_X, skip_clean, skip_resample)
    
    def _prep_timescale_X_y(self, interpolate_config, resample_agg_kwargs, timescale, timescale_encode=None, y_cols = ['energy'], exclude_from_X=[], skip_clean=False, skip_resample = False):
        encode_func = self._get_encode_func(timescale_encode)

        if skip_resample:
            temp_df = self._df.copy()
        else:
            temp_df = self._df.shark.resample(interpolate_config, resample_agg_kwargs, timescale, timescale_encode=timescale_encode, skip_clean=skip_clean)

        if encode_func:# add time_of_week_features
            temp_df = temp_df.pipe(encode_func, self._datetime_col)
            temp_df = temp_df.pipe(encode_holiday, self._datetime_col)# add holiday features

        X_cols = [c for c in temp_df.columns if c not in y_cols and c not in exclude_from_X]
        

        return temp_df.pipe(Xmat_y, x_cols=X_cols, y_cols=y_cols)

    def _prep_args_timescale(self, timescale):
        """helper function for preparing timescale dependent arguments"""
        timescale_resample = {'hourly': '1H', 'daily': '1D', 'weekly': '1W', 'monthly': '1M'}
        freq_check = {'hourly': 60, 'daily': 60*24, 'weekly': 60*24*7, 'monthly': 60*24*30}
        return timescale_resample[timescale], freq_check[timescale] 
    
    def _get_encode_func(self, timescale_encode=None):
        encode_func_dict = {'hour of week': encode_hour_of_week, 'hour of day': encode_hour_of_day,
                        'day of week': encode_day_of_week}
        encode_func = encode_func_dict[timescale_encode] if timescale_encode else None
        return encode_func

    
    def resample(self, interpolate_config, resample_agg_kwargs, timescale, timescale_encode=None, skip_clean=False):
        """skip_clean with skip interpolating and filling gap. This step is no longer needed if it is not a dirty frame."""
        timescale_resample, freq_check_val = self._prep_args_timescale(timescale)

        #set str for error msg
        timescale_msg = timescale_encode.split(' ')[0] if timescale_encode else timescale[:-2]
        timescale_encode = timescale_encode if timescale_encode else timescale
        
        freq = abs(int(td_to_minutes(likely_freq(self._df[self._datetime_col]))))
        #this probably should be in _prep_timescale_X_y but laziness
        if freq > freq_check_val: # fail if the person is using resolution greater than 1H such as 1D
            raise ValueError('Cant make {} features with data at freq over an {}!'.format(timescale_encode,timescale_msg))
        
        #scrub scurb, skrt skrt
        temp_df = self._df.copy() if skip_clean else self._df.shark.clean(interpolate_config)

        temp_df['count'] = 1 # make sure incomplete hours aren't included
        temp_df = temp_df.resample(timescale_resample, on = self._datetime_col)\
            .agg(self._resample_agg_config(resample_agg_kwargs, interpolate_config)).reset_index()
        
        if timescale == 'monthly': #prepare freq_check_val for monthly since different month differnt number of days
            freq_check_val = 60*24*temp_df[self._datetime_col].dt.daysinmonth
            temp_df[self._datetime_col] = temp_df[self._datetime_col].apply(lambda x: x.replace(day=1))

        complete_hour = freq_check_val / freq # how many points is a complete hour?
        temp_df = temp_df[temp_df['count'] == complete_hour].drop(['count'], axis = 1) # drop incomplete hours
        return temp_df