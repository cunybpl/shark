import numpy as np 
import pandas as pd
from pandas.api.extensions import register_dataframe_accessor
from .calc import TSFill, TSFindGaps, TSInterpolate, TSResample
from typing import List

@register_dataframe_accessor('shark')
class SharkDataFrame(object):
    """ different combinations of things. 
    """

    def __init__(self, df):
        self._df = self._validate(df)


    def _validate(self, df):
        '''
        Ensures the DF has a time column.
        '''
        
        if len(df.select_dtypes(include = ['datetime64']).columns) == 1:
            return df.copy(deep = True)
        else:
            raise ValueError("No datetime column in df")
    
    def find_gaps(self, time_column: str, freq: str) -> List:
        """find gaps given a dataframe with time_column

        Args:
            time_column (str): name of time column.
            freq (str): freq str

        Returns:
            [type]: [description]
        """        
        gaps = TSFindGaps(time_column)
        return gaps.find_gaps(self._df, freq)
    
    def fill(self, time_column: str, variable_columns: List, gaps_list: List) -> pd.DataFrame:
        """fill the gaps in time series given positions/datetime index where the gaps are occuring or the data are missing.
            The rest of the columns (excluding variable columns and time column) will be considered as metadata columns and
            they will be filled with metadata extracted from dataframe.

        Args:
            time_column (str): name of time column
            variable_columns (List): a list of variable columns. These are the columns that are filled with nan.
            gaps_list (List): a list of datetime index where data is missing

        Returns:
            pd.DataFrame: a dataframe with fillings.
        """        
        filling = TSFill(time_column, variable_columns)
        return filling.fill(self._df, gaps_list)

    def interpolate(self, time_column: str, add_flag: bool=True, **kwargs) -> pd.DataFrame:
        """interpolate the gaps in time series. Add flag to interpolated columns

        Args:
            time_column (str): the name of time column
            add_flag (bool): Defaults to True. If set to True, add a flag column that indicates if a point has been interpolated or not.
            kwargs: col_to_be_interpolated = dict of kwargs of pd.DataFrame.interpolate
                    example: kwargs = dict(var_x={'limit': 4, method: 'linear'}, var_y = {'limit': 5, method: 'polynomial'})
                    
        Returns:
            pd.DataFrame: an interpolated dataframe
        """        
        interpolation = TSInterpolate(time_column, **kwargs)
        return interpolation.interpolate(self._df, add_flag)

    def resample(self, time_column: str, timescale: str, metadata_cols: List=[], 
                    irregular: bool = False, num_limit_points: int = 0, **kwargs) -> pd.DataFrame:
        """resample time seires to a timescale.

        Args:
            time_column (str): name of time column.
            timescale (str): timescale such as hourly, daily, weekly, and monthly.
            metadata_cols (List): a list of metadata columns that will be included in output. 
            irregular (bool, optional): a flag to indicate whether incoming timeseries data is irregular;
                                        i.e timescales are not in consistent interval by any means.
                                        If it is set to True, as long as  numbers of points in an interval is greater than
                                        or equal to num_limit_points,points in that interval will be resampled and included in the final result.
                                        Defaults to False.
            num_limit_points (int, optional): If irregular is set to true, num_limit_points should be provided and set to at least 1.
                                            This is to ensure that incomplete interval(for example hours) aren't included.
                                            Defaults to 0.
            kwargs: col_to_be_resampled=aggregate function. kwargs is kwargs passed to pd.DataFrame.agg
                    Only columns provided in kwargs will be resampled. 
                    Other columns (excluding metadata columns and datetime cols) will not be in output.
                    example: kwargs = dict(var_x=np.sum, var_y=np.mean)

        Returns:
            pd.DataFrame: a resampled dataframe.
        """        
        resampling = TSResample(time_column, timescale, metadata_cols)
        return resampling.resample(self._df, irregular=irregular, num_limit_points=num_limit_points, **kwargs)