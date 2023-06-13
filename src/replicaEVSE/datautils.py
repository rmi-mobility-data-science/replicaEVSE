"""Data utilities for replica data. 
This module contains functions for loading 
and processing data."""

# import os
import dask.dataframe as dd
import pandas as pd

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
    path = path.replace('.csv', '.parquet')
    df.to_parquet(path)
    return df

def create_chunked_lists(chunk_size, person_list):
    """Creates list of person_id lists of a given chunk_size for parallelizing simulation
    Parameters
    ----------
    chunk_size : int
        Number of person ids to include in each sub-list
        The simulation runs each sub-list of people on a single core in series
    person_list : list
        Person ids to reorganize into list of lists
    Returns
    -------
    list
        List of lists of person ids
    """
    
    lists = [person_list[x:x+chunk_size] for x in range(0,len(person_list),chunk_size)]
    return lists

def sample_people_by_county(df: pd.DataFrame, ev_df: pd.DataFrame, year: str, fraction: float=0.05) -> pd.DataFrame:
    """ Selects a random sample of people (representing EVs) from each county.
    These numbers come from the stock rollover model.

    Args:
        df (_type_): _description_
        county (_type_): _description_
        num_to_select (_type_): _description_

    Returns:
        _type_: _description_
    """
    # get the unique people in the dataframe
    pop_df = df.drop_duplicates(subset=['person_id'])[['person_id', 'destination_county', 'building_type']]



    year = str(year)
    reduced_df = []
    for row_indexer, cnty in ev_df.iterrows():

        
        county = cnty['County']
        num_to_select = cnty[year]
        domicile = cnty['domicile']
        segment = cnty['Vehicle_type']
        engine = cnty['Powertrain']
        print(num_to_select, county, domicile, segment, engine)
        
        # there are negative numbers of vehicles?
        if num_to_select <= 0:
            num_to_select = 0
        
        # slice the unique dataframe to only include the county
        county_str = county +  ' County, WA'
        county_df = pop_df[pop_df['destination_county'] == county_str]

        # make sure we don't select more people than are in the county
        #if num_to_select > len(county_df):
        #    num_to_select = len(county_df)
        # print(f'Warning: {num_to_select} people selected for {county} but only {len(county_df)} people in {county}')

        if fraction is None:
            if domicile == 'sfh':
                county_df_sub = county_df[county_df['building_type'] == 'single_family']
            elif domicile == 'mfh':
                county_df_sub = county_df[county_df['building_type'] != 'single_family']
            else:
                print("Warning: domicile not recognized. Investigate the input df.")
            # unique people in that county
            selected = county_df_sub.person_id.sample(n=num_to_select, replace=False, random_state=42)
        else:
            # unique people in that county
            selected = county_df.person_id.sample(frac=fraction, replace=False, random_state=42)
        
        # grab only those selected people from the original dataframe
        cnty_df = df[(df['person_id'].isin(selected))].copy()

        # add the county, segment, and engine to the dataframe
        cnty_df.loc[row_indexer, 'engine'] = engine
        cnty_df.loc[row_indexer, 'segment'] = segment# .lower().replace(' ', '_').replace('/', '_')

        # append it to the reduced dataframe
        reduced_df.append(cnty_df)

    final_df = pd.concat(reduced_df)

    
    return final_df

def map_charge_type(row):
    """maps the destination to the type of charging station. Also maps the home 
    charging based on single family or multi family home as not all multi family
    homes with have access to a charger.

    Args:
        row (pd.DataFrame): row of the dataframe

    Returns:
        string: string to insert into the charge_type column
    """

    office_work_buildings = ['education', 'office', 'industrial', 'healthcare',]

    if row['travel_purpose'] == 'HOME':
        if row['building_type'] == 'single_family':
            return 'single_family_home'
        if row['building_type'] == 'mobile':
            return 'mobile_home'
        else:
            return 'multi_family_home'
    if row['travel_purpose'] == 'WORK':
        if row['destination_building_use_l2'] == 'civic_institutional':
            return 'civic_institutional'
        elif row['destination_building_use_l2'] in office_work_buildings:
            return 'office'
        else:
            return 'non_office_work'
    else:
        return 'public'