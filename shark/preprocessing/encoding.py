import pandas as pd
import numpy as np  
import holidays 


class _Encoder(object):

    def __init__(self, time_column):
        self.time_column = time_column 

    # XXX Potential refactor needed since those functions are pretty much the same thing
    # could expose more general encoding in the dashboard that way?

    # XXX DANGER ZONE XXX
    # when given less than a week / day
    # not all columns are generated !


    def encode_holidays(self, df, state='NY'):
        """ encode holidays by state
        """
        # make sure we didn't already encode
        temp = df[[c for c in df.columns if "day_off" != c]].copy(deep = True)
        hdays = holidays.CountryHoliday('US', state=state)
        temp['day_off'] = np.where(
            pd.to_datetime(temp[self.time_column]).dt.date\
                .apply(lambda x: x in hdays), 
            1, 
            0)
        return temp


    def encode_hour_of_day(self, df):
        """ create feature matrix using hour of day
        """
        # ensure we don't make duplicate encoding cols
        # drop the last col as it carries no new info
        return df[[c for c in df.columns if "hour_of_day" not in c]].join(
            pd.get_dummies(
                df[self.time_column].dt.hour + 1,
                prefix = "hour_of_day"
            ).drop(['hour_of_day_24'], axis = 1))

        
    
    def encode_hour_of_week(self, df):       
        """ create feature matrix using hour of week
        """
        # make sure we dont encode on encoded df
        # from the last dummy col as it carries no new info
        return df[[c for c in df.columns if "hour_of_week" not in c]].join(
            pd.get_dummies(
                df[self.time_column].dt.dayofweek * 24 + \
                    df[self.time_column].dt.hour + 1,
                prefix = "hour_of_week"
            ).drop(['hour_of_week_168'], axis = 1)
        )

    def encode_day_of_week(self, df):
        # make sure we dont encode on encoded df
        # from the last dummy col as it carries no new info
        return df[[c for c in df.columns if c not in \
            ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 
            'Friday', 'Saturday']
        ]].join(
            pd.get_dummies(
                df[self.time_column].dt.day_name()
            ).drop(['Sunday'], axis = 1)
        )

# expose callables to dataframe accessors/ pipeline

# XXX assert hour_of_day not in
# XXX assert is hourly data 
def encode_holiday(df, time_column, state='NY'):
    e = _Encoder(time_column)
    return e.encode_holidays(df, state=state)
    
def encode_hour_of_day(df, time_column): 
    e = _Encoder(time_column)
    return e.encode_hour_of_day(df)

def encode_hour_of_week(df, time_column):
    e = _Encoder(time_column)
    return e.encode_hour_of_week(df)

def encode_day_of_week(df, time_column):
    e = _Encoder(time_column)
    return e.encode_day_of_week(df)


