### shark

A dataframe extension for cleaning and resampling time series data

#### Usage

#### Shark pipeline
```python
from shark import schemas, shark
from numpy as np
data =[{'datetime': t.strftime('%Y-%m-%d %H:%M:%S'), 'a': 2, 'b':3, 'c': 'id'} for t in datetime_vec]
gap_ls = [data.pop(i) for i in [10]*4]#keeping record of gaps
gap_ls = [rec['datetime'] for rec in gap_ls] #keeping record of gaps

#fill config
fill_config = schemas.FillGapsConfig(time_column="datetime", freq='15T', variable_columns=['a','b'])

#interpolation config
func_args = schemas.PandasInterpolationFunc()
interpolation_func = schemas.InterpolationFunc(variable_name='a', func=func_args)
interpolate_config = schemas.InterpolationConfig(time_column="datetime",
                                interpolation_funcs=[interpolation_func])

#resample config 
resample_sum = schemas.ResampleFunc(variable_name='a', func=np.nansum)
resample_mean = schemas.ResampleFunc(variable_name='b', func=np.nanmean)
resample_funcs = [resample_sum, resample_mean]
resample_config = schemas.ResampleConfig(time_column="datetime", metadata_cols=['c'], timescale="hourly", resample_funcs=resample_funcs)

s = shark.Shark(data).fill(fill_config).interpolate(interpolate_config).resample(resample_config)
```

##### Finding gaps in time series
```python
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

```python
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
```python
interpolate_df = df_filled.shark.interpolate('datetime', add_flag=True, a={}, {'limit': 2}, add)

     a    b   c            datetime  a_interpolated  b_interpolated
0  2.0  3.0  id 2017-09-01 02:30:00               1               1
1  2.0  3.0  id 2017-09-01 02:45:00               1               1
2  2.0  NaN  id 2017-09-01 03:00:00               1               0

```


##### resampling
Pass columns to be resampled as kwargs along with respective functions and metadata columns as a list. 
```python
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

Resampling with irregular timeseries data
```python
irregular_data[['datestamp', 'air_temperature_observation_air_temperature']].head(20)
	datestamp	air_temperature_observation_air_temperature
datestamp	air_temperature_observation_air_temperature
0	2010-01-01 00:00:00	1.1
1	2010-01-01 00:51:00	1.1
2	2010-01-01 01:36:00	1.0
3	2010-01-01 01:51:00	1.1
4	2010-01-01 02:01:00	1.0
5	2010-01-01 02:37:00	1.0
6	2010-01-01 02:51:00	1.1
7	2010-01-01 03:00:00	1.1
8	2010-01-01 03:04:00	1.0
9	2010-01-01 03:51:00	0.6
10	2010-01-01 04:12:00	1.0
11	2010-01-01 04:49:00	1.0
12	2010-01-01 04:51:00	1.1
13	2010-01-01 04:59:00	NaN
14	2010-01-01 04:59:00	NaN
15	2010-01-01 05:51:00	0.6
16	2010-01-01 06:00:00	0.6
17	2010-01-01 06:04:00	1.0
18	2010-01-01 06:37:00	1.0
19	2010-01-01 06:47:00	1.0


#set irregular to True and num_limit_points to a non-zero positive integer so that only invtervals with
#enough data are included in final results
df = data.shark.resample(time_column='datestamp', timescale="hourly", 
                         metadata_cols=['usaf', 'wban', 'air_temperature_observation_air_temperature_unit'], 
                         irregular= True, num_limit_points=1,
                         air_temperature_observation_air_temperature=np.mean)
df.head(10)

	datestamp	air_temperature_observation_air_temperature	usaf	wban	air_temperature_observation_air_temperature_unit
0	2010-01-01 00:00:00	1.100000	725030	14732	degrees_celsius
1	2010-01-01 01:00:00	1.050000	725030	14732	degrees_celsius
2	2010-01-01 02:00:00	1.033333	725030	14732	degrees_celsius
3	2010-01-01 03:00:00	0.900000	725030	14732	degrees_celsius
4	2010-01-01 04:00:00	1.033333	725030	14732	degrees_celsius
5	2010-01-01 05:00:00	0.600000	725030	14732	degrees_celsius
6	2010-01-01 06:00:00	0.940000	725030	14732	degrees_celsius
7	2010-01-01 07:00:00	1.100000	725030	14732	degrees_celsius
8	2010-01-01 08:00:00	0.600000	725030	14732	degrees_celsius
9	2010-01-01 09:00:00	0.600000	725030	14732	degrees_celsius

#setting num_limit_points to 2 to not include hour 5
df = data.shark.resample(time_column='datestamp', timescale="hourly", 
                         metadata_cols=['usaf', 'wban', 'air_temperature_observation_air_temperature_unit'], 
                         irregular= True, num_limit_points=2,
                         air_temperature_observation_air_temperature=np.mean)
df.head(10)

	datestamp	air_temperature_observation_air_temperature	usaf	wban	air_temperature_observation_air_temperature_unit
0	2010-01-01 00:00:00	1.100000	725030	14732	degrees_celsius
1	2010-01-01 01:00:00	1.050000	725030	14732	degrees_celsius
2	2010-01-01 02:00:00	1.033333	725030	14732	degrees_celsius
3	2010-01-01 03:00:00	0.900000	725030	14732	degrees_celsius
4	2010-01-01 04:00:00	1.033333	725030	14732	degrees_celsius
6	2010-01-01 06:00:00	0.940000	725030	14732	degrees_celsius
9	2010-01-01 09:00:00	0.600000	725030	14732	degrees_celsius
12	2010-01-01 12:00:00	1.066667	725030	14732	degrees_celsius
13	2010-01-01 13:00:00	1.850000	725030	14732	degrees_celsius
14	2010-01-01 14:00:00	1.850000	725030	14732	degrees_celsius
```



##### Complete pipeline example

```python
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