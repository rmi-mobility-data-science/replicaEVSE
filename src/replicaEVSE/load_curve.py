""" This module contains functions to calculate the load curve of the charging"""

import pandas as pd
import numpy as np
# import replicaEVSE.sql_wrapper_functions as sql
# import dask.dataframe as dd
import dask


def _calculate_stop_duration(df: pd.DataFrame,
                            columns_list: list = None) -> pd.DataFrame:
    """ Calculate the charging duration of the trips. This is the time when 
    we would expect to be charging. We include the overnight duration. 
    This is an attempt to vectorize the calculation of the charging duration.

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


def _determine_charger_availability(
    person_df,
    trips_df
):
    """Depricated for now. Original version from John. Uses person data and trips data to determine 
    which types of charging the driver has access to, and what the order of 
    preference is. 

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
    # Notes
    # This should be revised to incorporate information about the person.
    # For example, the location of the home/work and type of home/work should inform whether
    # charging is available at these locations.
    # Also, some drivers may have different orders of preference for the
    # different charging options.

    dummy = person_df
    charge_set = list(set(trips_df.charge_type))
    charge_dict = {}
    # Before adding the options to the dictionary, check if the stop type exists in the trip data
    #  there are ranges over level 2, and DCFC are all public charging options
    if 'HOME' in charge_set:
        charge_dict.update({'HOME': 7.2})
    if 'WORK' in charge_set:
        charge_dict.update({'WORK': 7.2})
    if 'PUBLIC' in charge_set:
        charge_dict.update({'PUBLIC': 150})
    return charge_dict




def determine_charger_availability_tnc(df: pd.DataFrame, l2_frac: float = 0.5) -> dict:
    """ This will determine what kind on charger is available for the TNC trips. The replica data 
    doesn't track vehicles or drivers yet so this is just all the passengers. We are going to assume 
    the tnc drivers will charge every x trips and charge until they replenish the distance they 
    covered in those x trips. Assume a distribution of charger types where we can set the fraction 
    of each type.

    Args:
        level_2_frac (float): fraction of level 2 chargers. Defaults to 0.5. 
        DCFC will be 1 - level_2_frac. 

    Returns:
        dict: dict of type of charger the tnc uses in kw/h. 
    """
    charge_dict = {}
    dcfc_frac = 1 - l2_frac

    # create a distribution of charger types
    charger_list = [7.2]* int(l2_frac*100)
    charger_list = charger_list + [150]* int(dcfc_frac*100)

    # randomly select a charger type
    charger_type = np.random.choice(charger_list)
    if charger_type == 7.2:
        charge_dict.update({'HOME': 7.2})
    if charger_type == 150:
        charge_dict.update({'PUBLIC': 150})
    return charge_dict




def determine_charger_availability(
    trips_df,
    frac_work_charging=0.2,
    frac_non_office_charging=0.1,
    frac_civic_charging=0.5,
    frac_multiunit_charging=0.2,
    frac_singleunit_charging=1.0,
    frac_public_dcfc=0.9,
):
    """

    charge_types = ['multi_family_home', 'single_family_home', 'office', 'public',
    'civic_institutional', 'non_office_work', 'mobile']

    Parameters
    ----------
    trips_df : pandas DataFrame
        Trip data for one individual

    Returns
    -------
    dictionary
        Dictionary of available charging locations (HOME, WORK, and/or PUBLIC), with the power in kW for each option.
          The order of appearance in the dictionary sets the order of preference for the options.
    """



    charge_set = list(set(trips_df.charge_type))
    charge_dict = {}
    
    # Before adding the options to the dictionary, check if the stop type exists in the trip data
    #  there are ranges over level 2, and DCFC are all public charging options
    if 'single_family_home' in charge_set:
        # all homeowners of single fam have L2
        charge_dict.update({'single_family_home': 7.2})
    
    if 'multi_family_home' in charge_set:
        # dont charge if they dont have it
        mud_charge = np.random.choice([7.2, 0], p=[frac_multiunit_charging, 1-frac_multiunit_charging])
        charge_dict.update({'multi_family_home': mud_charge})
    
    if 'office' in charge_set:
        # use random choice to determine if worker can charge at work
        work_charge = np.random.choice([7.2, 0], p=[frac_work_charging, 1-frac_work_charging])
        charge_dict.update({'office': work_charge})
    
    if 'non_office_work' in charge_set:
        # use random choice to determine if worker can charge at work
        non_office_charge = np.random.choice([7.2, 0], p=[frac_work_charging, 1-frac_work_charging])
        charge_dict.update({'non_office_work': non_office_charge})
    
    if 'civic_institutional' in charge_set:
        # use random choice to determine if worker can charge at work
        civic_charge = np.random.choice([7.2, 0], p=[frac_civic_charging, 1-frac_civic_charging])
        charge_dict.update({'civic_institutional': civic_charge})
    
    if 'public' in charge_set:
        # use random choice to determine if worker can charge at work
        public_charge = np.random.choice([150, 19], p=[frac_public_dcfc, 1-frac_public_dcfc])
        charge_dict.update({'public': public_charge})
    
    if 'mobile_home' in charge_set:
        charge_dict.update({'mobile_home': 0})
    
    if len(charge_dict) == 0:
        charge_dict.update({'no_charge': 0})
    
    return charge_dict

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

def calculate_energy_available_per_stop(trips, consumption_kWh_per_mi, charger_availability):
    """Calculates the energy available per stop

    Parameters
    ----------
    trips : Pandas DataFrame
        trips dataframe for a single person_id

    Returns
    -------
    Pandas DataFrame
        trips dataframe with energy available per stop

    """
    # Initialize column for energy used per charge
    trips['charge_energy_used_kWh'] = 0
    # Calculate total miles driven for the day
    total_mi = float(np.sum(trips.distance_miles))
    # Calculate total energy consumption from total miles driven and energy consumption estimate
    total_energy = total_mi*consumption_kWh_per_mi
    # Initialize total energy to consume
    remaining_energy = total_energy
    
    # Use charge type to determine charger power available
    trips['charger_power_kW'] = [
        charger_availability[x] for x in trips.charge_type
    ]

    # Calculate total charge opportunity using stop duration and charger power
    # this is the total amount of energy that can be charged at each stop
    trips['charge_opportunity_remaining_kWh'] = [
        x[0].seconds/60/60*x[1] for x in zip(trips.stop_duration, trips.charger_power_kW)
    ]
    return trips, remaining_energy

def create_charging_events(
    df_trips,
    charger_availability,
    consumption_kWh_per_mi,
):
    """Creates dataframe of charging events
    Parameters
    ----------
    df_trips : Pandas DataFrame
        trips dataframe for a single person_id
    charger_availability : dictionary
        dictionary of available charging locations and charger power
        (e.g., {'HOME':7.2,'WORK':7.2,'PUBLIC':150})
    consumption_kWh_per_mi : float
        kWh/mi of the vehicle

    Returns
    -------
    Pandas DataFrame
        charging events

    """
    # prolly get rid of this since we do both days

    trips_dummy = df_trips.copy()
    
    # Note: make home overnight charging priority in the future
    trips = trips_dummy.sort_values(by='start_time')

    # Calculate energy available per stop
    # this is labeled as charge_opportunity_remaining_kWh since we subtract from it
    # as we charge. 
    trips, remaining_energy = calculate_energy_available_per_stop(trips, consumption_kWh_per_mi, charger_availability)

    # Initialize count variables
    i = 0 # charger type index
    j = 0 # stop index
    
    opportunities = True
    # Note: charge priority favors longest stops first
    # Allocate charge energy across available charge opportunities until all energy is
    # recharged or opportunities run out
    while (remaining_energy > 0) & (opportunities is True):
        charge_location = list(charger_availability.keys())[i]

        # by sorting by stop duration, we are prioritizing overnight and longer stops
        stops_sub = trips.loc[trips.charge_type == charge_location].sort_values(
            by='stop_duration', ascending=False).copy()
        if len(stops_sub) == 0:
            print(i, j, charge_location, remaining_energy, opportunities)

            break
        ind = stops_sub.index[j]
        
        # charge energy is the minimum of the remaining energy and the 
        # remaining charge opportunity
        charge_energy = np.min(
            [trips.loc[ind, 'charge_opportunity_remaining_kWh'], remaining_energy])
        
        # subtract the charge energy from the energy provided at that stop
        trips.loc[ind, 'charge_energy_used_kWh'] = charge_energy
        trips.loc[ind, 'charge_opportunity_remaining_kWh'] -= charge_energy

        remaining_energy = np.max([remaining_energy-charge_energy, 0])

        # move to next longest stop
        j += 1
        
        # if we've reached the end of the stops, move to the next charge location
        # until we get to the next charging type thus prioritizing whatever type
        # of charging level is first in the determine_charger_availability function. 
        if j == len(stops_sub):
            j = 0
            i += 1 # move to next charger type
        if i == len(charger_availability):
            opportunities = False
    
    # Return stop/charge info
    return (trips[[
        'activity_id',
        'charge_type',
        'charger_power_kW',
        'stop_duration',
        'charge_energy_used_kWh',
        'charge_opportunity_remaining_kWh'
    ]])


def distribute_charge(
    charge_demand,
    stop_time,
    stop_duration,
    time_window,
    charge_power,
    existing_load=None,
    managed=False
):
    # Notes
    # The managed charging option just optimizes for shaving peak demand.
    # Other possible approaches are cost minimization, emissions minimization, etc.
    # Also, rather than automatically selecting the lowest-demand periods to
    # allocate charging, this could be updated to distribute charging probabilistically,
    # with probability of selecting a window proportional to the difference between
    # the existing demand in that window and the daily peak demand.
    """Function to distribute a charging load across time, and minimize contribution to peak load if managed==True
    Parameters
    ----------
    stop_time : timedelta
        time of day when stop occurrs
    stop_duration : timedelta
        duration of stopping event
    time_window : timedelta
        duration of each segment over which to distribute charging (e.g., 15 minutes, 1 hr)
    charge_power : float
        capacity of charger
    existing_load : array
        array of existing load that is the same length as the charge array
    managed : Boolean
        Whether to shift charge to minimize load

    Returns
    -------
    array
        Array of length (1 day)/(time window duration) that includes kW demand per interval
    """

    # Make sure that the existing load data is the same dimensions as the
    # simulated load data. This could be modified to translate the existing
    # load data into the same dimensions as the simulated load data via interpolation
    if len(existing_load) != pd.Timedelta('1 day')/time_window:
        print('Existing load length != load curve output')


    # Initialize array of charge data for one person day
    charge_array = [0]*int(pd.Timedelta('1 day')/time_window)
    # Create array of window start times
    window_start_time = pd.to_timedelta(
        np.arange(0, 60*60*24, time_window.seconds), unit="s"
    )
    # Create dataframe for one person day of simulated charging
    load_df = pd.DataFrame({
        'window_start_time': window_start_time,
        'window_end_time': window_start_time+time_window
    })
    # Initialize daily energy demand
    remaining_charge = charge_demand
    # If managed==False, start the charge when the vehicle plugs in and charge
    # until the stop ends or the vehicle is fully charged
    if managed == False:
        start_index = int(np.floor(stop_time/time_window))
        proportion_of_start_window = (
            time_window-stop_time % time_window)/time_window
        charge_array[start_index] = np.min(
            [charge_power*proportion_of_start_window, remaining_charge/(time_window/pd.Timedelta('1 hour'))])
        remaining_charge -= charge_array[start_index] * \
            (time_window/pd.Timedelta('1 hour'))
        index = (start_index+1) % len(charge_array)
        while remaining_charge > 0:
            charge_array[index] = np.min(
                [charge_power, remaining_charge/(time_window/pd.Timedelta('1 hour'))])
            remaining_charge -= charge_array[index] * \
                (time_window/pd.Timedelta('1 hour'))
            index += 1
            index = index % len(charge_array)
        load_df['load_kW'] = charge_array
    # If managed==True, distribute the charge across the stopping event, starting with the window that coincides with the lowest existing demand, followed by the next lowest, etc.
    if managed == True:
        load_df['Load'] = existing_load
        load_df['Charge_window'] = [1]*len(existing_load)
        start_index = int(np.floor(stop_time/time_window))
        stop_index = int(np.floor((stop_time+stop_duration) /
                         time_window)) % len(charge_array)
        max_load = np.max(load_df.Load)
        if stop_index > start_index:
            load_df.loc[(load_df.index > stop_index) | (load_df.index < start_index),
                        'Charge_window'] = 0
        if stop_index < start_index:
            load_df.loc[(load_df.index > stop_index) & (load_df.index < start_index),
                        'Charge_window'] = 0
        load_df['Load_mod'] = load_df.Load*load_df.Charge_window
        load_df.loc[load_df.Load_mod == 0, 'Load_mod'] = max_load
        while remaining_charge > 0:
            index = load_df.loc[load_df.Load_mod ==
                                np.min(load_df.Load_mod)].index[0]
            if index == start_index:
                proportion_of_start_window = (
                    time_window-stop_time % time_window)/time_window
                charge_array[start_index] = np.min(
                    [charge_power*proportion_of_start_window, remaining_charge/(time_window/pd.Timedelta('1 hour'))])
                remaining_charge -= charge_array[start_index] * \
                    (time_window/pd.Timedelta('1 hour'))
            elif index == stop_index:
                proportion_of_stop_window = (
                    (stop_time+stop_duration) % time_window)/time_window
                charge_array[stop_index] = np.min(
                    [charge_power*proportion_of_stop_window, remaining_charge/(time_window/pd.Timedelta('1 hour'))])
                remaining_charge -= charge_array[stop_index] * \
                    (time_window/pd.Timedelta('1 hour'))
            else:
                charge_array[index] = np.min(
                    [charge_power, remaining_charge/(time_window/pd.Timedelta('1 hour'))])
                remaining_charge -= charge_array[index] * \
                    (time_window/pd.Timedelta('1 hour'))
            load_df.loc[index, 'Load_mod'] = max_load
        load_df['load_kW'] = charge_array
    # Return dataframe with charge windows and load per window for one person day
    return (load_df[['window_start_time',
                    'window_end_time',
                     'load_kW']])




def determine_energy_consumption(efficiency: float=0.3) -> float:
    """Determines energy consumption (kWh/mi) of the vehicle associated with a given person
    Parameters
    ----------
    person_df : pandas DataFrame
        Person data for one individual
    trips_df : pandas DataFrame
        Trip data for one individual

    Returns
    -------
    float
        Energy consumption rate (kWh/mi) of vehicle associated with a given person
    """
    return efficiency
    
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

def simulate_person_load(
    trips_df,
    existing_load,
    simulation_id,
    managed=False,
    efficiency=0.3,
    frac_work_charging=0.2,
    frac_non_office_charging=0.1,
    frac_civic_charging=0.5,
    frac_multiunit_charging=0.2,
    frac_singleunit_charging=1.0,
    frac_public_dcfc=0.9
):
    """Simulates loads for list of people
    Parameters
    ----------
    trips_df : Pandas DataFrame of trips and population data. 
        Should already be cut down to the mode and people per county.
    existing_load : Pandas DataFrame
        Existing load data frame
    simulation_id : string
        Identifier for simulation run. Year 
    managed : boolean
        Whether the charging is managed to reduce peak load (or for other objectives TBD)

    Returns
    -------
    dictionary
        Dictionary of dataframes:
            "charges" dataframe in which each row is a charge
            "loads" dataframe in which each row is a time window
    """

    # if using private auto, need to remove mobile home owners before you pass them
    # into this function. 
    #if (trips_df['mode'] == 'PRIVATE_AUTO').any():
    #    if len(trips_df = trips_df.loc[trips_df['building_type'] == 'mobile']) > 0:
    #        print('you need to remove the "building_type" == "mobile"')
    #        assert False
    
    if len(trips_df) == 0:
        print('empty trips dataframe')
        return ({'charges': 'No trips', 'loads': 'No trips'})
    
    # Create charge_type column from travel_purpose column
    # trips_df['charge_type'] = None
    trips_df['charge_type'] = trips_df.apply(map_charge_type, axis=1)

    trips_list = []
    loads_collection = []
    for j in list(set(trips_df.person_id)):
        # Get the subset of trips made by person j
        trips_temp = trips_df.loc[trips_df.person_id == j].copy()
        # Get the person data for person j
        # person_temp = persons_df.loc[persons_df.person_id == j].copy()
        person_temp = pd.DataFrame()
        # Determine vehicle energy consumpsion rate in kWh/mi
        vehicle_energy_consumption = efficiency

        charge_dfs = []
        for i in ['thursday', 'saturday']:
            if len(trips_temp.loc[trips_temp.weekday == i]) > 0:
                # For each day (thursday and saturday), get charger availability for person j
                # and determine which stopping events will result in charges

                trips_temp = calculate_stop_duration(trips_temp)
                trips_temp = trips_temp.loc[trips_temp['stop_duration'] > pd.to_timedelta('10 minutes')]

                charger_availability = determine_charger_availability(
                    trips_temp.loc[trips_temp.weekday == i],
                frac_work_charging,
                frac_non_office_charging,
                frac_civic_charging,
                frac_multiunit_charging,
                frac_singleunit_charging,
                frac_public_dcfc,
                    )
                charge_dfs += [
                    create_charging_events(
                        df_trips=trips_temp.loc[trips_temp.weekday == i].copy(
                        ),
                        charger_availability=charger_availability,
                        consumption_kWh_per_mi=vehicle_energy_consumption,
                    )
                ]
        # Concatenate results from the two days together
        charge_df = pd.concat(charge_dfs)
        charge_df['simulation_id'] = simulation_id
        # Create a unique ID for each charge
        charge_df['charge_id'] = [x[0]+'_'+x[1]
                                  for x in zip(charge_df.activity_id, charge_df.simulation_id)]
        charge_df['person_id'] = j
        # Merge charge data with trips data
        trips_temp = trips_temp.merge(charge_df)
        # #Create list of empty lists to fill with load data
        # trips_temp['Load']=[[]]*len(trips_temp)

        load_list = []
        for i in range(0, len(trips_temp)):
            # For each charge, distribute the load through time and the resulting df to the load df
            if trips_temp.charge_energy_used_kWh.iloc[i] > 0:
                weekday = trips_temp.weekday.iloc[i]
                load = distribute_charge(
                    charge_demand=trips_temp.charge_energy_used_kWh.iloc[i],
                    stop_time=trips_temp.end_time.iloc[i],
                    stop_duration=trips_temp.stop_duration.iloc[i],
                    time_window=pd.Timedelta('1 hour'),
                    charge_power=trips_temp.charger_power_kW.iloc[i],
                    
                    # TODO: this will always be false until we update the charge types
                    managed=managed if trips_temp.charge_type.iloc[i] in [
                        'HOME', 'WORK'] else False,
                    existing_load=existing_load.loc[existing_load.Weekday ==
                                                    weekday, 'D'].values
                )
                load['charge_id'] = trips_temp.charge_id.iloc[i]
                load['charge_type'] = trips_temp.charge_type.iloc[i]
                load['simulation_id'] = simulation_id
                load['person_id'] = trips_temp.person_id.iloc[i]
                load['load_segment_id'] = [
                    x[0]+'_'+str(x[1]) for x in zip(load.charge_id, load.index)]
                load_list += [load]
        if len(load_list) > 0:
            loads_collection += [pd.concat(load_list)]
            trips_list += [trips_temp]

    trips_df = pd.concat(trips_list)
    load_df = pd.concat(loads_collection)
    # Return charges and loads dataframes as a dictionary

    charges = trips_df[['person_id', 'charge_id', 'charge_type', 'activity_id', 'simulation_id',
                                  'charger_power_kW', 'charge_energy_used_kWh',
                                 'charge_opportunity_remaining_kWh', 'weekday']]
    
    loads = load_df[['person_id', 'load_segment_id', 'charge_id', 'charge_type', 'window_start_time', 'window_end_time', 'load_kW', 'weekday']]

    return {'charges': charges, 'loads': loads}

def simulate_person_load_tnc(
    trips_df,
    existing_load,
    simulation_id,
    managed=False
):
    """Simulates loads for list of people
    Parameters
    ----------
    person_ids : list
        List of person_ids
    database_connection : MySQL connector
        Connection to MySQL database
    person_columns : list
        Column names for person data table
    trips_columns : list
        Column names for trips table
    existing_load : Pandas DataFrame
        Existing load data frame
    simulation_id : string
        Identifier for simulation run
    managed : boolean
        Whether the charging is managed to reduce peak load (or for other objectives TBD)

    Returns
    -------
    dictionary
        Dictionary of dataframes:
            "charges" dataframe in which each row is a charge
            "loads" dataframe in which each row is a time window
    """

    # Take subset of trips using private autos as candidates for charging
    # trips_df = df.loc[df['mode'] == 'PRIVATE_AUTO'].copy()
    if len(trips_df) == 0:
        return ({'charges': 'No trips', 'loads': 'No trips'})

    trips_list = []
    loads_collection = []
    for j in list(set(trips_df.person_id)):
        # Get the subset of trips made by person j
        trips_temp = trips_df.loc[trips_df.person_id == j].copy()
        # Get the person data for person j
        # person_temp = persons_df.loc[persons_df.person_id == j].copy()
        person_temp = pd.DataFrame()
        # Determine vehicle energy consumpsion rate in kWh/mi
        # NOTE: this is currently a dummy function = 0.3
        vehicle_energy_consumption = determine_energy_consumption(
            person_temp, trips_temp)

        charge_dfs = []
        for i in ['thursday', 'saturday']:
            if len(trips_temp.loc[trips_temp.weekday == i]) > 0:
                # For each day (thursday and saturday), get charger availability for person j
                # and determine which stopping events will result in charges
                charger_availability = determine_charger_availability_tnc(level_2_frac=0.5)
                charge_dfs += [
                    create_charging_events(
                        df_trips=trips_temp.loc[trips_temp.weekday == i].copy(
                        ),
                        charger_availability=charger_availability,
                        consumption_kWh_per_mi=vehicle_energy_consumption,
                        weekday=i
                    )
                ]
        # Concatenate results from the two days together
        charge_df = pd.concat(charge_dfs)
        charge_df['simulation_id'] = simulation_id
        # Create a unique ID for each charge
        charge_df['charge_id'] = [x[0]+'_'+x[1]
                                  for x in zip(charge_df.activity_id, charge_df.simulation_id)]
        charge_df['person_id'] = j
        # Merge charge data with trips data
        trips_temp = trips_temp.merge(charge_df)
        # #Create list of empty lists to fill with load data
        # trips_temp['Load']=[[]]*len(trips_temp)

        load_list = []
        for i in range(0, len(trips_temp)):
            # For each charge, distribute the load through time and the resulting df to the load df
            if trips_temp.charge_energy_used_kWh.iloc[i] > 0:
                weekday = trips_temp.weekday.iloc[i]
                load = distribute_charge(
                    charge_demand=trips_temp.charge_energy_used_kWh.iloc[i],
                    stop_time=trips_temp.end_time.iloc[i],
                    stop_duration=trips_temp.stop_duration.iloc[i],
                    time_window=pd.Timedelta('1 hour'),
                    charge_power=trips_temp.charger_power_kW.iloc[i],
                    managed=managed if trips_temp.charge_type.iloc[i] in [
                        'HOME', 'WORK'] else False,
                    existing_load=existing_load.loc[existing_load.Weekday ==
                                                    weekday, 'D'].values
                )
                load['charge_id'] = trips_temp.charge_id.iloc[i]
                load['simulation_id'] = simulation_id
                load['person_id'] = trips_temp.person_id.iloc[i]
                load['load_segment_id'] = [
                    x[0]+'_'+str(x[1]) for x in zip(load.charge_id, load.index)]
                load_list += [load]
        if len(load_list) > 0:
            loads_collection += [pd.concat(load_list)]
            trips_list += [trips_temp]

    trips_df = pd.concat(trips_list)
    load_df = pd.concat(loads_collection)
    # Return charges and loads dataframes as a dictionary

    charges = trips_df[['person_id', 'charge_id', 'activity_id', 'simulation_id',
                                  'charger_power_kW', 'charge_energy_used_kWh',
                                 'charge_opportunity_remaining_kWh']]
    
    loads = load_df[['person_id', 'load_segment_id', 'charge_id', 'window_start_time', 'window_end_time', 'load_kW']]

    #return {'charges': trips_df[['charge_id', 'activity_id', 'simulation_id',
    #                              'charger_power_kW', 'charge_energy_used_kWh',
    #                             'charge_opportunity_remaining_kWh']],
    #        'loads': load_df[['load_segment_id', 'charge_id', 'window_start_time', 'window_end_time', 'load_kW']]}

    return {'charges': charges, 'loads': loads}

@dask.delayed
def simulate_person_load_dask(
    trips_df,
    existing_load,
    simulation_id,
    managed
):
    return simulate_person_load(trips_df,
        existing_load,
        simulation_id,
        managed)
