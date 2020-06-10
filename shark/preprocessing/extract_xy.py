import pandas as pd
import numpy as np

def Xmat_y(df, x_cols=[], y_cols=[], use_dfs=False):
    """ extract a numpy array or dataframes (for use with named ColumnTransformers) 
    """
    if len(y_cols) == 0:
        raise ValueError('y_cols must have at least 1 colname.')
    if len(x_cols) == 0:
        raise ValueError('x_cols must have at least 1 colname.')

    X = _extract_cols(df, x_cols, use_dfs)
    y = _extract_cols(df, y_cols, use_dfs, True)
    return X, y

def _extract_cols(df, cols, use_dfs, flattened=False):
    output = df[cols] if use_dfs else np.array(df[cols])
    if flattened and not use_dfs and len(cols) == 1:
        return df[cols[0]].values
    return output