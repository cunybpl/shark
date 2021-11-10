import pandas as pd
from shark.calc import TSFill

def test_fill(datetime_vec):
    data =[{'datetime': t, 'a': 2, 'b':3, 'c': 'id'} for t in datetime_vec]
    pop_1 = data.pop(10)
    pop_2 = data.pop(10)
    pop_3 = data.pop(10)
    gap_ls = [pop_1['datetime'], pop_2['datetime'], pop_3['datetime']]
    df = pd.DataFrame(data)
    filling = TSFill(time_column='datetime', variable_columns=['a', 'b'])
    exp_df = filling.fill(df, gap_ls)    
    
    records = exp_df.where((pd.notnull(exp_df)), None).to_dict(orient='records')
    records = [rec for rec in records if rec['datetime'] in gap_ls]
    assert len(records) == 3
    for rec in records:
        assert rec['a'] == None or pd.isna(rec['a'])
        assert rec['b'] == None or pd.isna(rec['a'])
        assert rec['c'] == 'id'

