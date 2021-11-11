from shark import schemas
import unittest
from pydantic import ValidationError

tc = unittest.TestCase()

def test_pandas_interpolation_func_schemas():
    pd_default_interpolation_func = schemas.PandasInterpolationFunc()
    assert pd_default_interpolation_func.method == 'linear'

    #test constraint on limit 
    with tc.assertRaises(ValidationError):
        schemas.PandasInterpolationFunc(limit=-1)

def test_interpolation_func_schemas():

    interpolation_func = schemas.InterpolationFunc(variable_name='a', func=schemas.PandasInterpolationFunc())

    assert interpolation_func.func.method == 'linear'
    assert interpolation_func.variable_name == 'a'

def test_interpolation_config():

    interpolation_func_a = schemas.InterpolationFunc(variable_name='a', func=schemas.PandasInterpolationFunc())
    interpolation_func_b = schemas.InterpolationFunc(variable_name='b', func=schemas.PandasInterpolationFunc(method='poly'))
    interpolate_config = schemas.InterpolationConfig(time_column="datetime",
                                    interpolation_funcs=[interpolation_func_a, interpolation_func_b])

    assert interpolate_config.interpolation_funcs[0].variable_name == 'a'
    assert interpolate_config.interpolation_funcs[0].func.method == 'linear'
    assert interpolate_config.interpolation_funcs[1].variable_name == 'b'
    assert interpolate_config.interpolation_funcs[1].func.method == 'poly'

    with tc.assertRaises(ValidationError):
        schemas.InterpolationConfig(time_column="datetime",
                                    interpolation_funcs=[])
def test_resample_func():
    
    resample_func = schemas.ResampleFunc(variable_name='a', func = lambda arr: arr[0])

    assert resample_func.func([1,2]) == 1
    assert resample_func.variable_name == 'a'

    with tc.assertRaises(ValidationError):
        schemas.ResampleFunc(variable_name='a', func = 'a')
    

def test_resample_config():
    
    resample_func = schemas.ResampleFunc(variable_name='a', func = lambda arr: arr[0])
    resample_config = schemas.ResampleConfig(time_column="datetime", timescale="hourly",
                                resample_funcs=[resample_func])

    assert resample_config.resample_funcs[0].variable_name == 'a'

    with tc.assertRaises(ValidationError):
        schemas.ResampleConfig(time_column="datetime", timescale="not hourly",
                                resample_funcs=[resample_func])

    with tc.assertRaises(ValidationError):
        schemas.ResampleConfig(time_column="datetime", timescale="hourly",
                                resample_funcs=[resample_func], num_limit_points = -1)
    with tc.assertRaises(ValidationError):
        schemas.ResampleConfig(time_column="datetime", timescale="hourly",
                                resample_funcs=[])