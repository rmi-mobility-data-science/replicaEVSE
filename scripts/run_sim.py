import pandas as pd
# import numpy as np
from joblib import Parallel, delayed
import replicaEVSE.sql_wrapper_functions as sql
import replicaEVSE.load_curve_simulation_functions as sim

datapath = '../../data/'


with open('../src/metadata/MySQLpwd.txt') as f:
    pw = f.readlines()
pw = pw[0]
connection_replica = sql.create_db_connection("localhost", "root", pw, 'replica')

gdf = pd.read_pickle('../../data/blockgroup_boundaries.pkl')

bgrp_list = list(gdf.GEOID.values)


def pull_person_list(connection, blockgroups):
    """Pulls list of person_ids from SQL database given a list of destination blockgroups.

    Blockgroups are smaller than census tracts, so this is a more accurate representation of the population in the area.
    
    Parameters
    ----------
    connection : MySQL connector
    blockgroups : list
        List of destination blockgroups. The person_id attached to any trip ending in the blockgroup will be used as part of the simulation population
        
    Returns
    -------
    list
        List of person_ids used in simulation
    """
    
    q1 = """
    SELECT person_id
    FROM trips
    WHERE destination_bgrp IN """+str(tuple(blockgroups))+""";
    """
    results_people = sql.read_query(connection, q1)
    persons_df = sql.convert_to_df(results_people,['person_id'])
    persons_df = persons_df.drop_duplicates()
    
    return list(set(persons_df.person_id))

person_list = pull_person_list(connection_replica, bgrp_list)

person_columns = [
    'household_id', 'person_id', 'BLOCKGROUP', 'BLOCKGROUP_work', 'age',
    'sex', 'race', 'ethnicity', 'individual_income', 'employment', 'education',
    'industry', 'household_role', 'commute_mode', 'household_size', 'household_income',
    'family_structure', 'vehicles', 'building_type', 'resident_type'
]
trips_columns = [
    'activity_id', 'person_id', 'distance_miles', 'mode', 'travel_purpose', 'start_time',
    'end_time', 'origin_bgrp', 'destination_bgrp', 'origin_land_use_l1', 'destination_land_use_l1',
    'origin_building_use_l1', 'destination_building_use_l1', 'vehicle_type','weekday'
]

#Created in the EIA_data_download.ipynb notebook
existing_load=pd.read_csv(datapath+'EIA_demand_summary.csv')


def create_lists(chunk_size, person_list):
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
    lists = [person_list[x:x+chunk_size] for x in range(0, len(person_list), chunk_size)]
    return lists

#Divide list of person_ids into 1000-person sub-lists for parallelization
chunked_list = create_lists(1000, person_list)

temp = Parallel(n_jobs=8)(
    delayed(sim.run_charge_simulation)(
        person_ids = i,
        database_name = 'replica',
        database_pw = pw,
        person_columns = person_columns,
        trips_columns = trips_columns,
        existing_load = existing_load,
        simulation_id = '01',
        managed = False
    ) for i in chunked_list)