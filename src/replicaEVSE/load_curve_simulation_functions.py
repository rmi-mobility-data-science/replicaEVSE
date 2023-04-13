"""Functions for simulating load curves for EV charging."""
import pandas as pd
import numpy as np
import replicaEVSE.sql_wrapper_functions as sql


def pull_travel_data(
    person_ids,
    connection,
    person_columns,
    trips_columns
):
    """Pull trip and population data for a given list of person_ids
    Parameters
    ----------
    person_ids : list
        List of person ids for which to pull trip data
    connection : database connection
    person_columns : list
        List of columns to pull from person data table
    trips_columns : list
        List of columns to pull from trips data table
    """
    # SQL query for trips data table
    q1 = """
    SELECT """+str(trips_columns).replace('[', '').replace(']', '').replace('\'', '')+"""
    FROM trips
    WHERE person_id IN """+str(tuple(person_ids))+""";
    """
    results_trips = sql.read_query(connection, q1)
    if isinstance(results_trips, list):  # Only proceed if the query returned something
        # These need to be sequential. Otherwise, len() will not work if results_trips is None
        if len(results_trips) > 0:
            trips = sql.convert_to_df(results_trips, trips_columns)
            # Pull population data for list of people
            q2 = """
            SELECT """+str(person_columns).replace('[', '').replace(']', '').replace('\'', '')+"""
            FROM population
            WHERE person_id IN """+str(tuple(person_ids))+""";
            """
            results_person = sql.read_query(connection, q2)
            person = sql.convert_to_df(results_person, person_columns)
            # Return dictionary with person data and trips data
            return ({'person': person, 'trips': trips})
    else:
        return (None)


def create_charging_events(
    df_trips,
    charger_availability,
    consumption_kWh_per_mi,
    weekday
):
    """Creates dataframe of charging events
    Parameters
    ----------
    df_trips : Pandas DataFrame
        trips dataframe for a single person_id
    charger_availability : dictionary
        dictionary of available charging locations and charger power
        ordered by preference (e.g., {'HOME':7.2,'WORK':7.2,'PUBLIC':150})
    consumption_kWh_per_mi : float
        kWh/mi of the vehicle

    Returns
    -------
    Pandas DataFrame
        charging events

    """
    dummy = weekday

    # Only select trips in private autos for passenger vehicle charging simulation
    trips_dummy = df_trips.loc[df_trips['mode'].isin(['PRIVATE_AUTO'])].copy()

    # Note: make home overnight charging priority in the future
    trips = trips_dummy.sort_values(by='start_time')
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
    trips['charge_opportunity_remaining_kWh'] = [
        x[0].seconds/60/60*x[1] for x in zip(trips.stop_duration, trips.charger_power_kW)
    ]
    # Initialize count variables
    i = 0
    j = 0
    opportunities = True

    # Note: charge priority should favor home charging
    # Allocate charge energy across available charge opportunities until all energy is
    # recharged or opportunities run out
    while (remaining_energy > 0) & (opportunities is True):
        charge_location = list(charger_availability.keys())[i]
        stops_sub = trips.loc[trips.charge_type == charge_location].sort_values(
            by='stop_duration', ascending=False).copy()
        ind = stops_sub.index[j]
        charge_energy = np.min(
            [trips.loc[ind, 'charge_opportunity_remaining_kWh'], remaining_energy])
        trips.loc[ind, 'charge_energy_used_kWh'] = charge_energy
        trips.loc[ind, 'charge_opportunity_remaining_kWh'] -= charge_energy

        remaining_energy = np.max([remaining_energy-charge_energy, 0])
        j += 1
        if j == len(stops_sub):
            j = 0
            i += 1
        if i == len(charger_availability):
            opportunities = False
    # Return stop/charge info
    return (trips[[
        'activity_id',
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
        return ('Error')

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


def determine_charger_availability(
    person_df,
    trips_df
):
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
    return (charge_dict)


def determine_energy_consumption(person_df, trips_df):
    # Notes
    # This is currently a dummy function. The person/trip data should be used to determine what the vehicle type is, and what the energy consumption per mile should be
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
    dummy = person_df
    dummy = trips_df
    return 0.3


def simulate_person_load(
    person_ids,
    database_connection,
    person_columns,
    trips_columns,
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
    print(person_ids[-1])
    # Pull the travel and person data
    travel_dfs = pull_travel_data(
        person_ids=person_ids,
        connection=database_connection,
        person_columns=person_columns,
        trips_columns=trips_columns
    )
    # If there is no data to pull, return "No trips"
    if not isinstance(travel_dfs, dict):
        return ({'charges': 'No trips', 'loads': 'No trips'})

    trips_df = travel_dfs['trips']
    persons_df = travel_dfs['person']
    # Take subset of trips using private autos as candidates for charging
    trips_df = trips_df.loc[trips_df['mode'] == 'PRIVATE_AUTO'].copy()
    if len(trips_df) == 0:
        return ({'charges': 'No trips', 'loads': 'No trips'})
    # Create charge_type column from travel_purpose column
    trips_df['charge_type'] = trips_df.travel_purpose.copy()
    trips_df.loc[trips_df.charge_type.isin(
        ['WORK', 'HOME']) == False, 'charge_type'] = 'PUBLIC'

    trips_list = []
    loads_collection = []
    for j in list(set(trips_df.person_id)):
        # Get the subset of trips made by person j
        trips_temp = trips_df.loc[trips_df.person_id == j].copy()
        # Get the person data for person j
        person_temp = persons_df.loc[persons_df.person_id == j].copy()
        # Determine vehicle energy consumpsion rate in kWh/mi
        # NOTE: this is currently a dummy function = 0.3
        vehicle_energy_consumption = determine_energy_consumption(
            person_temp, trips_temp)

        charge_dfs = []
        for i in ['thursday', 'saturday']:
            if len(trips_temp.loc[trips_temp.weekday == i]) > 0:
                # For each day (thursday and saturday), get charger availability for person j
                # and determine which stopping events will result in charges
                charger_availability = determine_charger_availability(
                    person_temp, trips_temp.loc[trips_temp.weekday == i])
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
                load['load_segment_id'] = [
                    x[0]+'_'+str(x[1]) for x in zip(load.charge_id, load.index)]
                load_list += [load]
        if len(load_list) > 0:
            loads_collection += [pd.concat(load_list)]
            trips_list += [trips_temp]

    trips_df = pd.concat(trips_list)
    load_df = pd.concat(loads_collection)
    # Return charges and loads dataframes as a dictionary
    return ({'charges': trips_df[['charge_id', 'activity_id', 'simulation_id',
                                  'charger_power_kW', 'charge_energy_used_kWh',
                                 'charge_opportunity_remaining_kWh']],
            'loads': load_df[['load_segment_id', 'charge_id', 'window_start_time', 'window_end_time', 'load_kW']]})


def run_charge_simulation(
    person_ids,
    database_name,
    database_pw,
    person_columns,
    trips_columns,
    existing_load,
    simulation_id,
    managed=False
):
    """Simulates loads for list of people
    Parameters
    ----------
    person_ids : list
        List of person_ids
    database_name : string
        Name of database to connect to
    database_pw : string
        MySQL password
    person_columns : list
        Column names for population table
    trips_columns : list
        Column names for trips table
    existing_load : Pandas DataFrame
        Existing load data frame
    simulation_id : string
        Identifier for simulation run
    managed : boolean
        Whether the charging is managed to reduce peak load (or for other objectives TBD)

    Does
    -------
    Populates charges table and loads table with simulated charging events and their loads
    """
    # Connect to database
    database_connection = sql.create_db_connection(
        "localhost", "root", database_pw, database_name)
    # Run simulation for list of person_ids
    sim_output = simulate_person_load(
        person_ids,
        database_connection,
        person_columns,
        trips_columns,
        existing_load,
        simulation_id,
        managed
    )
    # Populate SQL tables with simulation results
    if isinstance(sim_output['charges'], pd.DataFrame):
        sql.add_data_from_df(
            df=sim_output['charges'],
            table='charges',
            connection=database_connection,
            dtype_list=[
                str,  # charge_id VARCHAR(24) PRIMARY KEY,
                str,  # activity_id VARCHAR(20),
                str,  # simulation_id VARCHAR(4),
                float,  # charger_power_kW DECIMAL(2,1),
                float,  # charge_energy_used_kWh DECIMAL(4,1),
                float  # charge_opportunity_remaining_kWh DECIMAL(4,1)
            ]
        )
        sql.add_data_from_df(
            df=sim_output['loads'].loc[sim_output['loads'].load_kW > 0],
            table='loads',
            connection=database_connection,
            dtype_list=[
                str,  # load_segment_id VARCHAR(26) PRIMARY KEY,
                str,  # charge_id VARCHAR(24),
                str,  # window_start_time TIME,
                str,  # window_end_time TIME,
                float  # load_kW DECIMAL(5,1)
            ]
        )
    return ('')
