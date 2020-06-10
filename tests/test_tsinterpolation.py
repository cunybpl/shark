#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `shark` package."""



import unittest

from shark.preprocessing.tsinterpolation import TSInterpolate, add_delta_time_col, interpolate, fill
import pandas as pd
from pprint import pprint



## ported from tin's resample 
class TestIntervalDataClea(unittest.TestCase):

    def setUp(self):
        """"""
        self.datetime = pd.date_range('2017-09-01 00:00:00', '2017-09-02 00:00:00', freq='15T').to_list()
        self.idc = TSInterpolate('datetime', {'a': {}, 'b':{}})
    
    def test_remove_duplicates(self):
        time_vec = pd.to_datetime(['2017-09-01 00:00:00', '2017-09-02 00:00:00', '2017-09-02 00:00:00', '2017-09-03 00:00:00']).to_list()
        data = pd.DataFrame([{'datetime': t, 'a': 2, 'b':3} for t in time_vec])
        #self.idc.load_data(data)
        df = self.idc.remove_duplicates(data)
        self.assertEqual(len(df), 3)
    
    def test_duration_freq(self):
        data = [{'datetime': t, 'a': 2, 'b':3} for t in self.datetime]
        data.pop(10)

        df = pd.DataFrame(data)
        df_temp = add_delta_time_col(self.idc.remove_duplicates(df), self.idc.time_column)
        duration, freq = self.idc._get_duration_freq(df_temp)

        self.assertEqual(freq, 15)
    
    def test_make_interval_gap_frame(self):
        data =[{'datetime': t, 'a': 2, 'b':3} for t in self.datetime]
        data.pop(10)
        data.pop(10)
        data.pop(10)
        #self.idc.load_data(pd.DataFrame(data))
        df = pd.DataFrame(data)
        df_temp = add_delta_time_col(self.idc.remove_duplicates(df), 'datetime')
        _, freq = self.idc._get_duration_freq(df_temp)
        new_ts = self.idc._get_gaps_df(df_temp, str(freq)+'T')
        df_gaps = self.idc._make_interval_gap_frame(df, new_ts)
        records = df_gaps.where((pd.notnull(df_gaps)), None).to_dict(orient='records')

        self.assertEqual(3, len(records))
        for rec in records:
            self.assertEqual(rec['a'], None)
            self.assertEqual(rec['b'], None)

class TestExportableTSInterpolationMethod(unittest.TestCase):

    def setUp(self):
        datetime = pd.date_range('2017-09-01 00:00:00', '2017-09-01 12:00:00', freq='15T').to_list()
        data =[{'datetime': t, 'a': 2, 'b':3, 'c': 'id'} for t in datetime]
        [data.pop(i) for i in [10]*4]
        self.df = pd.DataFrame(data)

    def test_fill(self):
        df_fill = fill(self.df, 'datetime', {'a': {}, 'b':{}})
        self.assertEqual(len(df_fill), 49)
        self.assertEqual(len(self.df), 45)

        rec_fill = df_fill.where((pd.notnull(df_fill)), None).to_dict(orient='records')
        for i in [10,11,12,13]:
            self.assertEqual(rec_fill[i]['a'], None)
            self.assertEqual(rec_fill[i]['b'], None)

    def test_interpolate(self):
        self.assertEqual(len(self.df), 45)
        df_fill = fill(self.df, 'datetime', {'a': {}, 'b':{}})
        df_inter = interpolate(df_fill, 'datetime', {'a': {}, 'b':{'limit': 0}})
        
        rec_inter = df_inter.where((pd.notnull(df_inter)), None).to_dict(orient='records')
        for i in [10,11,12,13]:
            self.assertEqual(rec_inter[i]['a'], 2)
            self.assertEqual(rec_inter[i]['b'], None)