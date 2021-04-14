import operator
from typing import List

def add_delta_time_col(df, time_column):
    df['deltaTime'] = (df[time_column] - df[time_column].shift(-1))
    return df

def likely_freq(df):
    distribution_time_deltas = dict(df['deltaTime'].value_counts())
    ret = max(distribution_time_deltas.items(), key=operator.itemgetter(1))[0]
    return ret

def likely_freq_array(datetime_arr):
    # Get the time deltas
    datetime_arr = datetime_arr - datetime_arr.shift(+1)
    # Find the most frequent time delta
    return max(dict(datetime_arr.value_counts()).items(), key=operator.itemgetter(1))[0]

def td_to_minutes(duration):
    return float(duration.seconds/(60) + duration.days*24*60)


    