"""Data utilities for replica data. 
This module contains functions for loading 
and processing data."""

import dask.dataframe as dd
import os


def load_data(path, **kwargs):
    """Load data from a path.
    Parameters
    ----------
    path : str
        Path to the data.
    kwargs : dict
        Keyword arguments to pass to the dask.dataframe.read_csv function.
    Returns
    -------
    dask.dataframe.DataFrame
        The loaded data.
    """
    return dd.read_csv(path, dtype=str, **kwargs)

def clean_pop_data(pop_df):
    """There are some issues with the data
    where there are headers scattered throughout th
    rows. This function cleans the data and resets
    the data types.

    Args:
        pop_df (DataFrame): df or ddf of population data

    Returns:
        DataFrame: _description_
    """
    pop_df = pop_df[pop_df.person_id != 'person_id']
    dtype_dict = { 
              'age': int,
              'person_id': str,
              'household_id': str,
              'BLOCKGROUP': str,
              'BLOCKGROUP_work': str,
              'BLOCKGROUP_school': str,
              'lat': float,
              'lng': float,
              'lat_work': float, 
              'lng_work': float,
              'lat_school': float,
              'lng_school': float,      
              'household_income': float,
              'individual_income': float      
              }
    for col in pop_df.columns:
        if col not in dtype_dict.keys():
            dtype_dict[col] = str

    pop_df = pop_df.astype(dtype_dict)
    pop_df.set_index('person_id')
    return pop_df

def clean_trip_data(trip_df):
    trip_df = trip_df[trip_df.person_id != 'person_id']
    trip_dict = {}
    for col in trip_df.columns:
        if col == 'person_id':
            trip_dict[col] = str
        if col in ['start_time', 'end_time']:
            trip_dict[col] = 'timedelta64[ns]'
        
        elif col == 'distance_miles':
            trip_dict[col] = float
        elif 'lat' in col:
            trip_dict[col] = float
        elif 'lng' in col:
            trip_dict[col] = float
        else:
            trip_dict[col] = str
    trip_df = trip_df.astype(trip_dict)
    trip_df.set_index('person_id')
    return trip_df

def save_as_parquet(path, clean=True):
    """Save data as a parquet file.
    Parameters
    ----------
    path : str
        Path to the csv data.
    """
    if clean:
        if 'pop' in path:
            df = clean_pop_data(load_data(path))
        elif 'trip' in path:
            df = clean_trip_data(load_data(path))
    if '.parquet' in path:
        df.to_parquet(path)
    else:
        print("requires parquet file extension")
        # path = path.replace('.csv', '.parquet')
        # df.to_parquet(path)
    return df

