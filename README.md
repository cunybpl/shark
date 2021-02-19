### shark

A dataframe extension for cleaning and resampling time series data

#### Usage

##### Finding gaps in time series
```
import pandas as pd
import shark
#datetime list with 15 min frequency
datetime_ = pd.date_range('2017-09-01 00:00:00', '2017-09-02 00:00:00', freq='15T').to_list()
data =[{'datetime_col': t, 'a': 2, 'b':3} for t in datetime_vec]
[data.pop(10) for i in range(3)] #pop/delete three data points to create gaps
df = pd.DataFrame(data) #data frame with gaps
#get a list of time series gaps 
gaps_list = df.shark.find_gaps(time_column='datetime', freq='15T')
```

##### Filling gaps in time series
`pd.DataFrame.shark.fill(time_column, variable_columns, gaps_list)` will fill variable columns of a data frame with nans and
treat other columns (i.e df.columns - variable_columns - time_columns) as metadata columns and fill those metadata columns
with meta data.

```
import pandas as pd
import shark
#datetime list with 15 min frequency
datetime_ = pd.date_range('2017-09-01 00:00:00', '2017-09-02 00:00:00', freq='15T').to_list()
data =[{'datetime_col': t, 'a': 2, 'b':3} for t in datetime_vec]
[data.pop(10) for i in range(3)] #pop/delete three data points to create gaps
df = pd.DataFrame(data) #data frame with gaps

#first get a list of gaps in df
gaps_list = df.shark.find_gaps(time_column='datetime', freq='15T')

#gaps_list
print(gaps_list)
[Timestamp('2017-09-01 02:30:00', freq='15T'), Timestamp('2017-09-01 02:45:00', freq='15T'), Timestamp('2017-09-01 03:00:00', freq='15T')]

#now we can fill the gap, given position where gaps are occuring
df_filled = df.shark.fill(time_column='datetime', variable_columns=['energy'], gaps_list=gaps_list) 

#the gaps that were filled by shark

    a   b   c            datetime
0 NaN NaN  id 2017-09-01 02:30:00
1 NaN NaN  id 2017-09-01 02:45:00
2 NaN NaN  id 2017-09-01 03:00:00
```

##### Interpolating columns with NaN values
Below, we will interpolate `df_filled` dataframe, which has `a` and `b` as variable columns and `c` as metadata column.
We wish to interpolate `a` column with linear funciton and `b` column with linear function but only 2 consecutive gaps at a time. 
Please note that `shark.interpolate` wraps `pandas.DataFrame.interpolate`. Thus, by passing `a={}` as kwargs, `shark.interpolate` will default to default args in `pandas.DataFrame.interpolate` [method](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.interpolate.html), which uses `linear` function as default interpolating method. 
We can also set `add_flag` to false (defaults to true); this will not add columns that indicate whether a value has been interpolated or not.
```
interpolate_df = df_filled.shark.interpolate('datetime', add_flag=True, a={}, {'limit': 2}, add)

     a    b   c            datetime  a_interpolated  b_interpolated
0  2.0  3.0  id 2017-09-01 02:30:00               1               1
1  2.0  3.0  id 2017-09-01 02:45:00               1               1
2  2.0  NaN  id 2017-09-01 03:00:00               1               0

```


##### resampling
Pass columns to be resampled as kwargs along with respective functions and metadata columns as a list. 
```
data =[{'datetime': t, 'a': 2, 'b':3, 'c': 'id', 'd': 'boo', 'e':5} for t in datetime_]
df_hourly = pd.DataFrame(data)

#note here that since we don't proivde 
df = df_hourly.shark.resample(time_column='datetime', timescale="hourly", metadata_cols=['c', 'd'], a=np.sum, b=np.mean)
df.head()
             datetime  a  b   c    d
0 2017-09-01 00:00:00  8  3  id  boo
1 2017-09-01 01:00:00  8  3  id  boo
2 2017-09-01 02:00:00  8  3  id  boo
3 2017-09-01 03:00:00  8  3  id  boo
4 2017-09-01 04:00:00  8  3  id  boo
```
Note here that 'e' is not included in final output df since we didn't provide anything about 'e'.


##### Complete pipeline example

```
#rtm_df is a dataframe with datetime column named 'datetime', a variable column named 'energy' and an extra column named 'demand'
#get a list of timeseries index where gaps occured 
gap_ls = rtm_df.shark.find_gaps(time_column='datetime', freq='15T') 
#use gap_list to fill missing energy data with nan and other cols with meta data
df_filled = rtm_df.shark.fill(time_column='datetime', variable_columns=['energy'], gaps_list=gap_ls) 
#now fill nan energy data with interpolate linear function; see pd.DataFrame.interpolate for args
interpolate_df = df_filled.shark.interpolate(time_column='datetime', add_flag=False, energy={}) #note energy is kwargs here
#now resample clean dataframe to hourly; note energy is kwargs here
resample_df = interpolate_df.shark.resample(time_column='datetime', timescale='hourly', energy=np.mean)
```