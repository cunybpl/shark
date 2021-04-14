from shark.calc import TSFindGaps
import pandas as pd 
from .fixtures import datetime_vec

def test_detect_freq(datetime_vec):
    data = [{'datetime': t, 'a': 2, 'b':3} for t in datetime_vec]
    data.pop(10)

    gaps = TSFindGaps(time_column="datetime")
    df = pd.DataFrame(data)
    freq = gaps._detect_freq(df, freq='')

    assert freq == '15.0T'

def test_make_interval_gap_frame(datetime_vec):
    data =[{'datetime': t, 'a': 2, 'b':3} for t in datetime_vec]
    pop_1 = data.pop(10)
    pop_2 = data.pop(10)
    pop_3 = data.pop(10)
    exp_gap_ls = [pop_1['datetime'], pop_2['datetime'], pop_3['datetime']]
    #self.idc.load_data(pd.DataFrame(data))
    df = pd.DataFrame(data)

    gaps = TSFindGaps(time_column="datetime")
    gap_ls = gaps.find_gaps(df, freq='15T')

    assert len(gap_ls) == 3
    assert set(gap_ls) == set(exp_gap_ls)