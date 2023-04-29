"""This script runs the simulation for the Replica EVSE project. See the notebooks for details on how to get to this step."""

import pandas as pd
import numpy as np
import dask.dataframe as dd
import replicaEVSE.load_curve as sim
import os
import joblib
import multiprocessing as mp
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

#Created in the EIA_data_download.ipynb notebook
existing_load=pd.read_csv(os.path.join(datadir, 'EIA_demand_summary.csv'))

test = False


# Start the stopwatch / counter 
load_start_time = process_time() 

# use dask?
use_dask = False
if use_dask:

    from dask.distributed import Client, LocalCluster
    from dask.diagnostics import ProgressBar

    # start a local cluster and get the client address
    cluster = LocalCluster(n_workers=int(0.9 * mp.cpu_count())) # Launches a scheduler and workers locally
    client = Client(cluster)  # Connect to distributed cluster and override default
    print(client)
    pbar = ProgressBar()
    pbar.register()


    # load the joined data for trips that end in washington and people from the 
    # population simulation. This uses dask but if you have a ton of memory you could try 
    # loading it with pandas. 
    ddf = dd.read_parquet(os.path.join(datadir, 'wa_pop_and_trips.parquet'))

    # right now, only look at private auto trips
    ddf = ddf.loc[ddf['mode'] == mode]

    # sort on person_id and start_time
    # ddf = ddf.sort_values(by=['person_id', 'start_time', 'weekday']).reset_index(drop=True)

    # ddf = ddf.reset_index(drop=True)

    # convert to pandas dataframe
    # necessary for the next steps
    print('---computing the dask data frame---')
    df = ddf.compute()
else:
    if test:
        df = pd.read_parquet(os.path.join(datadir, 'wa_pop_and_trips_subsample.parquet'))
        df = df.loc[df['mode'] == mode]
    else:
        print('---reading the data frame---')
        df = pd.read_parquet(os.path.join(datadir, 'wa_pop_and_trips.parquet'))
        df = df.loc[df['mode'] == mode]

# Stop the stopwatch / counter
load_stop_time = process_time()

print("time to load:", load_stop_time - load_start_time) 

    # df = df.head(10000)

chunck_start_time = process_time()
# chunck_size = int(len(df)/number_of_chunks)
print('---breaking the dataframe into chuncks---')
# break the dataframe into chuncks
# must be a df, not a ddf
df_list = np.array_split(df, number_of_chunks)
chunck_stop_time = process_time()
print("time to break into chuncks:", chunck_stop_time - chunck_start_time)

sim_start_time = process_time()
# run the simulation in parallel
# df must be a pandas dataframe
charge_sims = joblib.Parallel(verbose=10, n_jobs=-1)(
    joblib.delayed(sim.simulate_person_load)(
    df=df,
    existing_load=existing_load,
    simulation_id='base',
    managed=False
) for df in df_list)
sim_stop_time = process_time()
print("time to simulate:", sim_stop_time - sim_start_time)

# restack the dataframes
# use dask to make use of the parallelization?
print('---restacking the dataframes---')
stack_start_time = process_time()
charges_list = [x['charges'] for x in charge_sims]
charges_df = pd.concat(charges_list)
loads_list = [x['loads'] for x in charge_sims]
loads_df = pd.concat(loads_list)
stack_stop_time = process_time()
print("time to stack:", stack_stop_time - stack_start_time)


# save the results
print('---saveing the charges and loads dfs---')
save_start_time = process_time()
charges_df.to_parquet(os.path.join(datadir, f'charges_{mode}_{simulation_id}.parquet'))
loads_df.to_parquet(os.path.join(datadir, f'loads_{mode}_{simulation_id}.parquet'))
save_stop_time = process_time()
print("time to save:", save_stop_time - save_start_time)

# close the client
if use_dask:
    client.close()