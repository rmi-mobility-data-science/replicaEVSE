""" This module contains functions to calculate the load curve of the charging"""

import pandas as pd
# import dask.dataframe as dd
import dask


def calculate_stop_duration(df: pd.DataFrame,
                            columns_list: list = None) -> pd.DataFrame:
    """ Calculate the charging duration of the trips. This is the time when 
    we would expect to be charging. We include the overnight duration

    Args:
        df (pd.dataframe): pd dataframe of the trips

    Returns:
        pd.dataframe: trips dataframe with the charging duration column
    """

    if columns_list is not None:
        df = df[columns_list]

    # Calculate the time between trips
    df['stop_duration'] = df['start_time'] - df['end_time'].shift()

    # Calculate the stop duration for the first trip of the day
    first_stop_time = df.groupby('person_id')['end_time'].min().to_frame(
        'first_stop_time')
    first_start_time = df.groupby('person_id')['start_time'].min().to_frame(
        'first_start_time')
    first_trip = first_start_time.join(first_stop_time).reset_index()
    first_trip['stop_duration'] = first_trip['first_start_time'] - first_trip[
        'first_stop_time']
    df = pd.merge(
        df,
        first_trip[['person_id', 'stop_duration', 'first_start_time']],
        on='person_id',
        how='left')

    # Calculate the stop duration for the last trip of the day
    df['last_stop_duration'] = df.groupby(
        'person_id')['stop_duration_x'].shift(-1)
    df.loc[df['start_time'] == df.groupby('person_id')['start_time'].transform('max'), 'last_stop_duration'] = \
        df.groupby('person_id')['first_start_time'].transform('max') - \
            df.loc[df['start_time'] == df.groupby('person_id')['start_time'].transform('max'), 'end_time']

    # Set the stop duration for the first trip of the day to the last stop duration
    df.loc[df['start_time'] == df.groupby('person_id')['start_time'].transform('min'), 'stop_duration'] = \
        df.groupby('person_id')['last_stop_duration'].shift()

    # Replace the first element of stop_duration with the last_stop_duration for each person
    df.loc[df.groupby('person_id')['start_time'].transform('idxmin'), 'stop_duration'] = \
        df.groupby('person_id')['last_stop_duration'].shift()

    # For the last stop of the day, assume next start time is same as first start time of the day
    df['stop_duration'] = df[['stop_duration',
                              'last_stop_duration']].max(axis=1)

    # Remove the days from the stop duration
    df['stop_duration'] = df['stop_duration'] - pd.to_timedelta(
        df['stop_duration'].dt.days, unit='d')

    # Drop the temporary columns
    df = df.drop([
        'stop_duration_x', 'stop_duration_y', 'first_start_time',
        'last_stop_duration'
    ],
                 axis=1)

    return df


def determine_charger_availability(
        df: dask.dataframe,) -> dask.dataframe:
    # Notes
    # This should be revised to incorporate information about the person.
    # For example, the location of the home/work and type of home/work should inform whether
    # charging is available at these locations.
    # Also, some drivers may have different orders of preference for the
    # different charging options.
    """Uses person data and trips data to determine which types of charging the driver has access to, and what the order of preference is
    Parameters
    ----------
    person_df : pandas DataFrame
        Person data for one individual
    trips_df : pandas DataFrame
        Trip data for one individual

    Returns
    -------
    dictionary
        Dictionary of available charging locations (HOME, WORK, and/or PUBLIC), with the power in kW for each option. The order of appearance in the dictionary sets the order of preference for the options.
    """
    charge_set = df.charge_type.unique()
    charge_dict = {}
    # Before adding the options to the dictionary, check if the stop type exists in the trip data
    #  there are ranges over level 2, and DCFC are all public charging options
    if 'HOME' in charge_set:
        charge_dict.update({'HOME': 7.2})
    if 'WORK' in charge_set:
        charge_dict.update({'WORK': 7.2})
    if 'PUBLIC' in charge_set:
        charge_dict.update({'PUBLIC': 150})
    return (charge_dict)