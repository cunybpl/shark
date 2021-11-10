from shark import schemas
from shark.shark import Shark, _unpack_interpolation_funcs, _unpack_resample_funcs, _convert_str_to_datetime
import numpy as np
import datetime

def test_unpack_interpolation_funcs():
    func_a = schemas.InterpolationFunc(variable_name='a', func=schemas.PandasInterpolationFunc())
    func_b = schemas.InterpolationFunc(variable_name='b', func=schemas.PandasInterpolationFunc())
    func_c = schemas.InterpolationFunc(variable_name='c', func=schemas.PandasInterpolationFunc())
    unpacked = _unpack_interpolation_funcs([func_a, func_b, func_c])

    for key in ['a', 'b', 'c']:
        assert key in unpacked.keys() 


def test_unpack_resample_funcs():
    func_a = schemas.ResampleFunc(variable_name='a', func=lambda x: x)
    func_b = schemas.ResampleFunc(variable_name='b', func=lambda x: x)
    func_c = schemas.ResampleFunc(variable_name='c', func=lambda x: x)
    unpacked = _unpack_resample_funcs([func_a, func_b, func_c])

    for key in ['a', 'b', 'c']:
        assert key in unpacked.keys() 

def test_convert_str_to_datetime():
    data = [{'a': '2017-07-01', 'b': 1},{'a': '2017-07-02', 'b': 2}]

    data_converted = _convert_str_to_datetime(data, key='a', format="%Y-%m-%d")
    assert data_converted[0]['a'] == datetime.datetime.strptime('2017-07-01', "%Y-%m-%d")
    assert data_converted[0]['b'] == 1
    assert data[0]['a'] == '2017-07-01'
    assert data[0]['b'] == 1

    assert data_converted[1]['a'] == datetime.datetime.strptime('2017-07-02', "%Y-%m-%d")
    assert data_converted[1]['b'] == 2
    assert data[1]['a'] == '2017-07-02'
    assert data[1]['b'] == 2 
    
def test_shark_pipeline(datetime_vec):
    data =[{'datetime': t.strftime('%Y-%m-%d %H:%M:%S'), 'a': 2, 'b':3, 'c': 'id'} for t in datetime_vec]
    gap_ls = [data.pop(i) for i in [10]*4]
    gap_ls = [rec['datetime'] for rec in gap_ls]

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

    s = Shark(data).fill(fill_config).interpolate(interpolate_config).resample(resample_config)
    
    assert s.data.data_filled != None
    assert s.data.data_interpolated != None
    assert s.data.data_resampled != None
    assert s.current_stage == 'data_resampled'

    s = Shark(data)
    assert s.current_stage == 'init'

    s.fill(fill_config)
    assert s.current_stage == "data_filled"

    s.resample(resample_config)
    assert s.current_stage == "data_resampled"
    assert s.data.data_interpolated == None
    assert s.data.data_resampled != None

