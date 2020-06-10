import pandas as pd
import numpy as np
from shark.preprocessing.extract_xy import Xmat_y
import unittest

class TestXmaty(unittest.TestCase):
    def setUp(self):
        self.x1 = [1]*10
        self.x2 = [2]*10
        self.y1 = [11]*10
        self.y2 = [22]*10

    def test_use_dfs_true(self):
        df = pd.DataFrame({'x1': self.x1, 'x2': self.x2, 'y1': self.y1, 'y2': self.y2})

        #multi columns
        x,y = Xmat_y(df, ['x1', 'x2'], ['y1', 'y2'], use_dfs=False)

        self.assertTrue(isinstance(x,np.ndarray))
        self.assertTrue(isinstance(y,np.ndarray))

        for i in list(x):
            self.assertListEqual(list(i),[1,2])
        
        for i in list(y):
            self.assertListEqual(list(i),[11,22])
        
        #single columns
        x,y = Xmat_y(df, ['x1'], ['y1'], use_dfs=False)

        for i in list(x):
            self.assertListEqual(list(i),[1])
        
        for i in list(y):
            self.assertEqual(i,11)
    
    def test_use_dfs_false(self):
        df = pd.DataFrame({'x1': self.x1, 'x2': self.x2, 'y1': self.y1, 'y2': self.y2})

        #multi columns
        x,y = Xmat_y(df, ['x1', 'x2'], ['y1', 'y2'], use_dfs=True)
        
        self.assertTrue(isinstance(x,pd.DataFrame))
        self.assertTrue(isinstance(y,pd.DataFrame))

        #test values
        self.assertListEqual(list(x['x1']), self.x1)
        self.assertListEqual(list(x['x2']), self.x2)
        self.assertListEqual(list(y['y1']), self.y1)
        self.assertListEqual(list(y['y2']), self.y2)

        #test length
        self.assertEqual(len(list(x)), 2)
        self.assertEqual(len(list(y)), 2)

        #test single columns
        x,y = Xmat_y(df, ['x1'], ['y1'], use_dfs=True)
        print('x')
        self.assertListEqual(list(x['x1']), self.x1)
        self.assertListEqual(list(y['y1']), self.y1)

        print('df')
        self.assertEqual(len(list(x)), 1)
        self.assertEqual(len(list(y)), 1)
    
    def test_empty_zero_cols_input_rasie_value_error(self):
        with self.assertRaises(ValueError):
            df = pd.DataFrame({'x1': self.x1, 'x2': self.x2, 'y1': self.y1, 'y2': self.y2})
            x,y = Xmat_y(df, ['x1', 'x2'], [], use_dfs=True)

        with self.assertRaises(ValueError):
            df = pd.DataFrame({'x1': self.x1, 'x2': self.x2, 'y1': self.y1, 'y2': self.y2})
            x,y = Xmat_y(df, [], [], use_dfs=True)