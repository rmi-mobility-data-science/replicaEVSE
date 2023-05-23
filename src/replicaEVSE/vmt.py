import dask.dataframe as dd
import pandas as pd


def calc_vmt_by_county(df: pd.DataFrame,
                       mode: str,
                       weekday: str,
                       county_key: str = 'home_cty'
                       ) -> pd.DataFrame:
    """
    Calculate VMT by county for a given mode

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame with columns ['county', 'vmt', 'mode']
    mode : str
        Mode to filter by
    weekday : str
        'saturday' or 'thursday'

    Returns
    -------
    pandas.DataFrame
        DataFrame with vmt per county
    """
    df = df[df['mode'] == mode]
    df = df[df['weekday'] == weekday]
    df = df.groupby(county_key)['distance_miles'].sum().reset_index()
    return df
