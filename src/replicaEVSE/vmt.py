import dask.dataframe as dd
import pandas as pd

def calc_vmt_by_county(df, mode):
    """
    Calculate VMT by county for a given mode

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame with columns ['county', 'vmt', 'mode']
    mode : str
        Mode to filter by

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns ['county', 'vmt', 'mode']
    """
    df = df[df['mode'] == mode]
    df = df.groupby('home_cty')['distance_miles'].sum().reset_index()
    return df
