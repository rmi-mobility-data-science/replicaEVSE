"""This script runs the simulation for the Replica EVSE project. See the notebooks for details on how to get to this step."""

import pandas as pd
import numpy as np
# import dask.dataframe as dd
import replicaEVSE.load_curve as sim
import os
import joblib
from time import process_time


# user defined variables
# where is the data
datadir = '../../data'
simulation_id = 'base'
mode = 'PRIVATE_AUTO'

# number of chunks to break the dataframe into
# this is used to parallelize the simulation
# note: data set is ~50 million rows
number_of_chunks = 1000 # 10000 rows in each chunck
test = False



#Created in the EIA_data_download.ipynb notebook
existing_load=pd.read_csv(os.path.join(datadir, 'EIA_demand_summary.csv'))


# Start the stopwatch / counter 
load_start_time = process_time() 



if test:
    df = pd.read_parquet(os.path.join(datadir, 'wa_pop_and_trips_subsample.parquet'))
    df = df.loc[df['mode'] == mode]
    simulation_id = 'test'
else:
    print('---reading the data frame---')
    df = pd.read_parquet(os.path.join(datadir, 'wa_pop_and_trips_sorted.parquet'))
    df = df.loc[df['mode'] == mode]

# Stop the stopwatch / counter
load_stop_time = process_time()

print("time to load:", (load_stop_time - load_start_time)/60, "minutes")

    # df = df.head(10000)

chunck_start_time = process_time()
# chunck_size = int(len(df)/number_of_chunks)
print('---breaking the dataframe into chuncks---')
# break the dataframe into chuncks
# must be a df, not a ddf
df_list = np.array_split(df, number_of_chunks)
chunck_stop_time = process_time()
print("time to break into chuncks:", (chunck_stop_time - chunck_start_time)/60, "minutes")

sim_start_time = process_time()
# run the simulation in parallel
# df must be a pandas dataframe
charge_sims = joblib.Parallel(verbose=10, n_jobs=-1)(
    joblib.delayed(sim.simulate_person_load)(
    trips_df=df,
    existing_load=existing_load,
    simulation_id='base',
    managed=False
) for df in df_list)
sim_stop_time = process_time()
print("time to simulate:", (sim_stop_time - sim_start_time)/60, "minutes")


# restack the dataframes
# use dask to make use of the parallelization?
print('---restacking the dataframes---')
stack_start_time = process_time()
charges_list = [x['charges'] for x in charge_sims]
charges_df = pd.concat(charges_list)
loads_list = [x['loads'] for x in charge_sims]
loads_df = pd.concat(loads_list)
stack_stop_time = process_time()
print("time to stack:", (stack_stop_time - stack_start_time)/60, "minutes")


# save the results
print('---saveing the charges and loads dfs---')
save_start_time = process_time()
charges_df.to_parquet(os.path.join(datadir, f'charges_{mode}_{simulation_id}.parquet'))
loads_df.to_parquet(os.path.join(datadir, f'loads_{mode}_{simulation_id}.parquet'))
save_stop_time = process_time()
print("time to save:", (save_stop_time - save_start_time)/60, "minutes")
