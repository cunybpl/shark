import pandas as pd
import numpy as np
import operator

def likely_freq(datetime_arr):
    # Get the time deltas
    datetime_arr = datetime_arr - datetime_arr.shift(+1)
    # Find the most frequent time delta
    return max(dict(datetime_arr.value_counts()).items(), key=operator.itemgetter(1))[0]

def td_to_minutes(duration):
  '''
  Takes timedelta object from datetime library and converts it to float number of mins
  '''
  return float(duration.seconds/(60) + duration.days*24*60)

def custom_resampler(df, interval_start_col_name, period_end_dates, period_start_dates = None):
    """
    Returns groupby object since it's responsibility of the user to make sure each col is properly grouped.
    It makes the function more flexible.
    """
    # so this return output very similar to df.resample when
    # used with period_end_dates and period_start_dates that are same as
    # 1M / 1W / 1D etc frequencies
    # but it is a little different
    # TODO: add kwargs like closed and label as well as outputting the dt label
    # to make behavior same as pandas resample
    # and then write tests showing
    # assert_frame_equal(custom_resample(**kwargs) == resample(**kwargs)) at 
    # different frequencies

    # Ensure input arrays are pd DateTime
    period_end_dates = pd.to_datetime(period_end_dates)
    period_start_dates = pd.to_datetime(period_start_dates)
    # Ensure input arrays are sorted
    period_end_dates = period_end_dates.sort_values()
    period_start_dates = period_start_dates.sort_values()
    # Ensure df is sorted
    df = df.sort_values(interval_start_col_name)
    
    # Make sure incomplete periods are excluded from the output (using start and end dates)
    df = df[df[interval_start_col_name] < period_end_dates[-1]]
    df = df[df[interval_start_col_name] >= period_start_dates[0]]
    if df[interval_start_col_name][0] > period_start_dates[0]:
        df = df[df[interval_start_col_name] >= period_start_dates[1]]
    
    # Figure out likely frequency
    freq = likely_freq(df[interval_start_col_name])
    # Ensure there are no gaps, and if there are, interpolate them
    df = df.set_index(interval_start_col_name).resample(freq).mean().reset_index().interpolate('linear')
    # Assign a period to each reading
    df['period'] = (period_end_dates - freq).values.searchsorted(df[interval_start_col_name])
    # Resample on those periods
    return df.groupby(by = ['period'])