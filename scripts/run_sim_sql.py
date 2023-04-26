import pandas as pd
import numpy as np
from datetime import datetime
from joblib import Parallel, delayed
import replicaEVSE.sql_wrapper_functions as sql
import replicaEVSE.load_curve_simulation_functions as sim

now = datetime.now()
print('Start time: ', now.strftime("%H:%M:%S"))
datadir = '../../data'
with open('../src/metadata/MySQLpwd.txt', 'r') as f:
    pw = f.readlines()
pw = pw[0]
connection_replica = sql.create_db_connection("localhost", "root", pw, 'replica')


# get list of blockgroups
gdf = pd.read_pickle(datadir+'/blockgroup_boundaries.pkl')
bgrp_list = list(gdf.GEOID.values)


# sql query the trips table to get all the people who have trips to WA
blockgroups = bgrp_list
q1 = """
    SELECT person_id
    FROM trips
    WHERE destination_bgrp IN """+str(tuple(blockgroups))+""";
    """


# get all the people taking trips to WA
print('Pulling person list from SQL database')
results_people = sql.read_query(connection_replica, q1) # 80s
people_list = list(set(results_people)) # 12s

# break it up for parallel processing
print('Breaking up person list into chunks')
chunked_list = np.array_split(people_list, 1000)

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
existing_load=pd.read_csv('../../data/EIA_demand_summary.csv') 


# drop the charge tables from previous runs
sql.execute_query(connection_replica, 'DROP TABLE IF EXISTS loads;')
sql.execute_query(connection_replica, 'DROP TABLE IF EXISTS charges;')
sql.execute_query(connection_replica, 'DROP TABLE IF EXISTS simulation_runs;')

create_charges_table="""
CREATE TABLE charges (
  charge_id VARCHAR(24) PRIMARY KEY,
  activity_id VARCHAR(20),
  simulation_id VARCHAR(4),
  charger_power_kW DECIMAL(5,1),
  charge_energy_used_kWh DECIMAL(4,1),
  charge_opportunity_remaining_kWh DECIMAL(5,1)
  );
"""
create_load_table="""
CREATE TABLE loads (
  load_segment_id VARCHAR(26) PRIMARY KEY,
  charge_id VARCHAR(24),
  window_start_time TIME,
  window_end_time TIME,
  load_kW DECIMAL(5,1)
  );
"""
create_simulation_table="""
CREATE TABLE simulation_runs (
  simulation_id VARCHAR(4) PRIMARY KEY,
  simulation_date DATETIME,
  simulation_attributes VARCHAR(200)
  );
"""

sql.execute_query(connection_replica, create_charges_table) # Execute our defined query
sql.execute_query(connection_replica, create_load_table) # Execute our defined query
sql.execute_query(connection_replica, create_simulation_table) # Execute our defined query

"""# run the simulations
print('Running simulations')
now = datetime.now()
print('Start time: ', now.strftime("%H:%M:%S"))
temp = Parallel(verbose=100)(
    delayed(sim.run_charge_simulation)(
        person_ids = i,
        database_name = 'replica',
        database_pw = pw,
        person_columns = person_columns,
        trips_columns = trips_columns,
        existing_load = existing_load,
        simulation_id = '02',
        managed = True
    ) for i in chunked_list)#"""


# run the simulations
print('Running simulations')
now = datetime.now()
print('Start time: ', now.strftime("%H:%M:%S"))
sim.run_charge_simulation(
        person_ids = chunked_list[0][0:100],
        database_name = 'replica',
        database_pw = pw,
        person_columns = person_columns,
        trips_columns = trips_columns,
        existing_load = existing_load,
        simulation_id = 'test',
        managed = False
    ) 
