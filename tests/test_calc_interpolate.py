import pandas as pd
from shark.calc import TSInterpolate, TSFill

def test_interpolate(datetime_vec):
    data =[{'datetime': t, 'a': 2, 'b':3, 'c': 'id'} for t in datetime_vec]
    gap_ls = [data.pop(i) for i in [10]*4]
    gap_ls = [rec['datetime'] for rec in gap_ls]
    df = pd.DataFrame(data)

    filling = TSFill(time_column='datetime', variable_columns=['a', 'b'])
    df = filling.fill(df, gap_ls)

    interpolation = TSInterpolate(time_column='datetime',  a={})
    df_inter = interpolation.interpolate(df)
    records = [rec for rec in df_inter.to_dict(orient="records") if rec['datetime'] in gap_ls]
    rec_inter = df_inter.where((pd.notnull(df_inter)), None).to_dict(orient='records')

    for i in [10,11,12,13]:
        assert rec_inter[i]['a'] == 2
        assert rec_inter[i]['b'] == None or pd.isna(rec_inter[i]['b'])