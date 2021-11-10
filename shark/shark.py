from .extension import SharkDataFrame
from .schemas import FillGapsConfig, GenericRecords, ResampleFunc, SharkObject, InterpolationFunc, \
                                InterpolationConfig, ResampleConfig
import datetime
import pandas as pd
from typing import List, Dict, Union

def _unpack_resample_funcs(funcs: List[ResampleFunc]) -> Dict[str, callable]:
    return {func.variable_name: func.func for func in funcs}

def _unpack_interpolation_funcs(funcs: List[InterpolationFunc]) -> Dict[str, Dict]:
    return {func.variable_name: func.dict() for func in funcs}

class Shark(object):

    def __init__(self, data: GenericRecords):
        self._current_stage = 'init'
        self._current_data = None
        self.data = SharkObject(data=data)
        self._set_current_data(data, key = 'data')

    def _set_current_data(self, data, key: str):
        self._current_data = self.data.__getattribute__(key)

    def _add_result_to_data(self, data: GenericRecords, key: str):
        data_to_add = {**self.data.dict(), key: data}
        self.data = SharkObject(**data_to_add)

    def get_current_data(self):
        return self._current_data
    
    def get_current_stage(self):
        return self.current_stage
    
    def _set_current_stage(self, stage: str):
        self._current_stage = stage

    def fill(self, config: FillGapsConfig):
        data = self._convert_str_to_datetime(self._current_data, key=config.time_column, format=config.format)
        df = pd.DataFrame(data)
        gaps_list = df.shark.find_gaps(time_column=config.time_column, freq=config.freq)
        df_filled = df.shark.fill(time_column=config.time_column, variable_columns=config.variable_columns, gaps_list=gaps_list)
        data_filled = df_filled.to_dict(orient="records")
        self._add_result_to_data(data_filled, key = 'data_filled')
        self._set_current_data(data_filled, key = 'data_filled')
        self._set_current_stage('data_filled')
        return self

    def interpolate(self, config: InterpolationConfig):
        if not self.data.data_filled:
            data = self._convert_str_to_datetime(self._current_data,  key=config.time_column, format=config.format)
        else:
            data = self.get_current_data()
        df_filled = pd.DataFrame(data)
        interpolate_df = df_filled.shark.interpolate(config.time_column, add_flag=config.interpolation_flag, 
                                            **_unpack_interpolation_funcs(config.interpolation_funcs))
        interpolated_data = interpolate_df.to_dict(orient="records")
        self._add_result_to_data(interpolated_data, key = 'data_interpolated')
        self._set_current_data(interpolated_data, key = 'data_interpolated')
        self._set_current_stage('data_interpolated')
        return self

    def resample(self, config: ResampleConfig):
        if not self.data.data_interpolated:
            data = self._convert_str_to_datetime(self._current_data,  key=config.time_column, format=config.format)
        else:
            data = self.get_current_data()
        interpolated_df = pd.DataFrame(data)
        resample_df = interpolated_df.shark.resample(time_column=config.time_column, timescale=config.timescale, 
                                                irregular=config.irregular, num_limit_points=config.num_limit_points,
                                                metadata_cols=config.metadata_cols,
                                                **_unpack_resample_funcs(config.resample_funcs))
        resampled_data = resample_df.to_dict(orient="records")
        self._add_result_to_data(resampled_data, key = 'data_resampled')
        self._set_current_data(resampled_data, key = 'data_resampled')
        self._set_current_stage('data_resampled')
        return self
    

    def _convert_str_to_datetime(self, records: GenericRecords, key: str, format: str) -> GenericRecords:
        """convert value corresponds to key for a list of records from datetime to string

        Args:
            records (GenericRecords): List of records
            key (str): the name of the key of the value that is to be converted to datetime.
            format (str): the format of the input datetime str.

        Returns:
            GenericRecords: converted GenericRecords
        """        
        map_ = map(lambda x: {key: datetime.datetime.strptime(x.pop(key), format), **x}, records)
        return list(map_)