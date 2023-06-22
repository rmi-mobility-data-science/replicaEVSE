"""Data utilities for replica data. 
This module contains functions for loading 
and processing data."""

# import os
import dask.dataframe as dd
import pandas as pd
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

    lists = [person_list[x:x+chunk_size]
             for x in range(0, len(person_list), chunk_size)]
    return lists


def calculate_stop_duration(trips_df: pd.DataFrame) -> pd.DataFrame:
    """ TODO: implement this before making the big combined trips table. 
    This needs to be run on a single weekday so do it before stacking. Might still need
    to loop through each person_id. Slow but only needs to run once. Or try a groupby. 
    
    Note: this is currently not employed, choosing to stick with 
    the old looping method for now. 

    Args:
        trips_df (pd.DataFrame): replica trip data table

    Returns:
        pd.DataFrame: updated table with new stop_duration column
    """
    
    trips = trips_df.sort_values(by='start_time')
    # Initialize stop_duration column
    trips['stop_duration'] = 0
    # For each row in the trips table, calculate the stop duration
    for i in range(0, len(trips)-1):
        trips.iloc[i, trips.columns.get_loc('stop_duration')] =\
            trips.iloc[i+1, trips.columns.get_loc('start_time')] -\
            trips.iloc[i, trips.columns.get_loc('end_time')]
    # For the last stop of the day, calculate the stop duration assuming the next start
    # time is the same as the start time of the first trip of the day
    trips.iloc[len(trips)-1, trips.columns.get_loc('stop_duration')] =\
        trips.iloc[0, trips.columns.get_loc('start_time')] -\
        (trips.iloc[len(trips)-1, trips.columns.get_loc('end_time')
                    ]-pd.to_timedelta('1 day'))
    return trips


def segment_efficiency(segment):
    """This simply returns the expected efficiency (kwh/mi) for a given segment.
    It is quite simple now but could be expanded to include more complex
    efficiency calculations over the years.

    Args:
        segment (str): The segment of the vehicle

    Returns:
        float: efficiency in kWh/mile
    """

    # get the efficiency for the segment
    if "Sedan" in segment:
        eff = 0.25
    elif "Crossover" in segment:
        eff = 0.30
    elif 'Truck' in segment:
        eff = 0.49
    else:
        print('no match')
    return eff


def phev_efficiency_milage(df, engine):
    """This function returns the efficiency of a PHEV based on the
    number of miles driven on electricity. The efficiency set at 0.9 kWh/mile
    for now. We also change the milage for these users. 

    Args:
        df (pd.DataFrame): row of the dataframe
        engine (str): engine type, PHEV or EV
    """
    if engine == 'PHEV':
        df['distance_miles'] = df['distance_miles'] * 0.55
        df['efficiency'] = 0.9
    return df


def exclusionary_sampler(df: pd.DataFrame, population_df: pd.DataFrame, nev_df: pd.DataFrame, county: str, year: str) -> pd.DataFrame:
    """Takes in a dataframe of trips and a dataframe of population and samples
    expecting this dataset has residents from one county only. 

    Args:
        df (pd.DataFrame): _description_
        population_df (pd.DataFrame): population df for the people in given county
        nev_df (pd.DataFrame): _description_
        county (str): _description_
        year (str): _description_

    Returns:
        pd.DataFrame: _description_
    """

    # subset the nev_df to only include the county
    nvehicles_sub = nev_df[nev_df['County'] == county].copy()

    # Create a list to keep track of selected individuals for each combination
    already_sampled_people = []

    # Create a list to store the cnty dataframes.
    cnty_df_list = []

    # Iterate over the county DataFrame and sample individuals from the population DataFrame
    for _, row in nvehicles_sub.iterrows():
        county = row['County']
        vehicle_type = row['Vehicle_type']
        domicile = row['domicile']
        count = row[year]
        # engine = row['Powertrain']
        powertrain = row['Powertrain']

        if count < 0:
            count = 0

        # slice the datafrane to only include people with the correct domicile
        if domicile == 'sfh':
            domicile_cond = population_df['building_type'] == 'single_family'
        else:
            domicile_cond = population_df['building_type'] != 'single_family'

        # filter the county population based on domicile
        filtered_population = population_df[(domicile_cond)]
        print(
            f'Number of people in county and domicile = {domicile}: {filtered_population.shape[0]}')

        # exclude already selected individuals for this combination
        already_sampled_cond = filtered_population['person_id'].isin(
            already_sampled_people)
        filtered_people = filtered_population[~already_sampled_cond].copy()

        print(
            f'Filtered population not in the previous sample: {domicile} = {filtered_people.shape[0]}')
        # sample 'count' number of individuals
        sampled_population = filtered_people.sample(
            n=count, replace=False, random_state=42)
        sampled_individuals = sampled_population['person_id'].to_list()

        # Update the selected individuals list
        already_sampled_people.extend(sampled_individuals)

        print(f'Sampled {count} individuals from County: {county}, Vehicle Type: {vehicle_type}, Domicile: {domicile}, Powertrain: {powertrain} \n')

        # grab only those selected people with this combination
        # of county, domicile, and vehicle type and powertrain.
        cnty_df = df[(df['person_id'].isin(sampled_individuals))].copy()
        cnty_df['engine'] = powertrain
        cnty_df['segment'] = vehicle_type
        cnty_df['efficiency'] = segment_efficiency(vehicle_type)
        cnty_df['year'] = str(2022)
        ctny_df = phev_efficiency_milage(cnty_df, powertrain)
        ctny_df['charge_type'] = ctny_df.apply(map_charge_type, axis=1)
        cnty_df_list.append(cnty_df)

    full_county_df = pd.concat(cnty_df_list)
    return full_county_df


def sample_people_by_county(df: pd.DataFrame, ev_df: pd.DataFrame, year: str) -> pd.DataFrame:
    """ NOTE: Why are we doing this loop twice????
    
    We already loop through the 
    counties in the ev_df. 
    
    Selects a random sample of people (representing EVs) from each county.
    These numbers come from the stock rollover model.

    Args:
        df (_type_): _description_
        county (_type_): _description_
        num_to_select (_type_): _description_

    Returns:
        _type_: _description_
    """
    # get the unique people in the dataframe
    total_population_df = df.drop_duplicates(subset=['person_id'])[
        ['person_id', 'home_cty', 'building_type']]

    year = str(year)
    reduced_df = []
    county_list = ev_df['County'].unique()
    for county in county_list:
        
        # slice the unique dataframe to only include people in that county
        county_str = county + ' County, WA'
        county_cond = total_population_df['home_cty'] == county_str 
        county_pop_df = total_population_df[county_cond].copy()       

        #pop_df = df.drop_duplicates(subset=['person_id'])[
        #['person_id', 'home_cty', 'building_type']]

        # run the sampler for each county
        cnty_df = exclusionary_sampler(df, county_pop_df, ev_df, county, year)

        # append it to the reduced dataframe
        reduced_df.append(cnty_df)

    final_df = pd.concat(reduced_df)

    return final_df


def run_and_save_sampled_populations(df, nev_df, year, datadir='../../data/'):
    """Run the county sampler for a given year from the output of the stock rollover
    model and save the year-on-year outputs.

    Args:
        df (DataFrame): Full trips dataframe
        nev_df (DataFrame): Output of the stock rollover model
        year (int): year to sample
        datadir (_type_): _description_

    """
    simulation_id = f'{str(year)}'
    df_county_subset = sample_people_by_county(df, nev_df, year=year)
    df_county_subset.to_parquet(os.path.join(
        datadir, f'county_samples/county_sample_{simulation_id}.parquet'))


def map_charge_type(row):
    """maps the destination to the type of charging station. Also maps the home 
    charging based on single family or multi family home as not all multi family
    homes with have access to a charger.

    Args:
        row (pd.DataFrame): row of the dataframe

    Returns:
        string: string to insert into the charge_type column
    """

    office_work_buildings = ['education',
                             'office', 'industrial', 'healthcare',]

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
