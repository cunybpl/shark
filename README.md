=======
shark
=======
A dataframe extension for cleaning and resampling time series data

Example:

```
#rtm_df is a dataframe with datetime column named 'datetime'
#get a list of timeseries index where gaps occured 
gap_ls = rtm_df.shark.find_gaps(time_column='datetime', freq='15T') 
#use gap_list to fill missing energy data with nan and other cols with meta data
df_filled = rtm_df.shark.fill(time_column='datetime', variable_columns=['energy'], gaps_list=gap_ls) 
#now fill nan energy data with interpolate linear function; see pd.DataFrame.interpolate for args
interpolate_df = df_filled.shark.interpolate(time_column='datetime', energy={}) #note energy is kwargs here
#now resample clean dataframe to hourly; note energy is kwargs here
resample_df = interpolate_df.shark.resample(time_column='datetime', timescale='hourly', energy=np.mean)
```