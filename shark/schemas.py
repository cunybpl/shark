import pydantic
from typing import List, Dict, Any, Optional, runtime_checkable, Union
from typing_extensions import Protocol
import numpy as np
from dataclasses import dataclass

GenericRecords = List[Dict[str, Any]]

@runtime_checkable
class ResampleFuncCallable(Protocol): 
    def __call__(self, arr: Union[List[Union[float, str]], np.ndarray]) -> Union[float, str]: ...

class SharkBaseConfig(pydantic.BaseModel):
    time_column: str
    format: str = "%Y-%m-%d %H:%M:%S"

class FillGapsConfig(SharkBaseConfig):
    freq: str
    variable_columns: List[str]

    # not quite sure
    # @pydantic.validator('interpolation_funcs')
    # def check_variable_columns_not_empty(cls, v):
    #     if len(v) == 0:
    #         raise ValueError("interpolation_funcs cannot be an empty list")
    #     return v


#this is not a model for a function but rather signatures/args for the function
#this model is from https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.interpolate.html
class PandasInterpolationFunc(pydantic.BaseModel):
    method: str = 'linear'
    limit: Optional[pydantic.conint(ge=0)]
    limit_direction: Optional[str]
    limit_area: str = None
    downcast: str = None
    kwargs: Dict[str, Any] = {}

class InterpolationFunc(pydantic.BaseModel):
    variable_name: str
    func: PandasInterpolationFunc

class InterpolationConfig(SharkBaseConfig):
    interpolation_flag: bool = False
    interpolation_funcs: List[InterpolationFunc]

    @pydantic.validator('interpolation_funcs')
    def check_interpolation_funcs_not_empty(cls, v):
        if len(v) == 0:
            raise ValueError("interpolation_funcs cannot be an empty list")
        return v

class ResampleFunc(pydantic.BaseModel):
    variable_name: str
    func: ResampleFuncCallable #this cannot be fully validated

    class Config:
        arbitrary_types_allowed = True


class ResampleConfig(SharkBaseConfig):
    timescale: str
    metadata_cols: List[str]=[]
    irregular: bool = False
    num_limit_points: pydantic.conint(ge=0)=0
    resample_funcs: List[ResampleFunc]

    @pydantic.validator('timescale')
    def timescale_validator(cls, v):
        valid = ['hourly', 'daily', 'weekly', 'monthly']
        if v not in valid:
            raise ValueError(f'{v} is not an accepted timescale.')
        return v 
    
    @pydantic.validator('num_limit_points')
    def num_limits_points(cls, v, values, **kwargs):
        if values['irregular'] and v < 1:
            raise ValueError(f"If irregular is set to True, num_limit_points: {v} must be greater than 1.")
        return v
    
    @pydantic.validator('resample_funcs')
    def check_resample_funcs_not_empty(cls, v):
        if len(v) == 0:
            raise ValueError("resample_funcs cannot be an empty list")
        return v


@dataclass
class SharkObject:
    data: GenericRecords
    data_filled: Optional[GenericRecords] = None
    data_interpolated: Optional[GenericRecords] = None
    data_resampled: Optional[GenericRecords] = None

    def dict(self):
        return self.__dict__