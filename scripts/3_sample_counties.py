import pandas as pd
import numpy as np
import replicaEVSE.load_curve as sim
import replicaEVSE.datautils as simdu
import os
import joblib

datadir = '../../data/'
mode = 'PRIVATE_AUTO'

print('========== Running sample_counties.py ==========')
print('========== Loading data ==========')
# read in the joined trips and population data sets
merged_df = pd.read_parquet(os.path.join(datadir, 'wa_pop_and_trips_sorted_county.parquet'))

# right now, only look at private auto trips
df = merged_df.loc[merged_df['mode'] == mode]
# take out the mobile and commercial MHDV

### TODO: revisit taking out mobile home owners
df = df[(df['building_type'] != 'mobile') & (df['building_type'] != None)]

# get the stock rollover data
stock_rollover = pd.read_csv(datadir+'ldv_population_output_adjusted.csv')
ev_cond = stock_rollover['Powertrain'].isin(['EV', 'PHEV']).reset_index(drop=True)
nev_df = stock_rollover[ev_cond].copy()
nev_df = nev_df[nev_df['domicile'] != 'other'].copy()
nev_df.drop(columns=['Unnamed: 0'], inplace=True)

print('========== Runnign parallel sampling by year ==========')
# run the sampler for each year
year_list = np.arange(2022, 2036, 1)
if True:

    joblib.Parallel(verbose=10, n_jobs=4)(joblib.delayed(simdu.run_and_save_sampled_populations)(
        df,
        nev_df, 
        year,
        datadir,
        ) for year in year_list)
else:
    for year in year_list:
        simdu.run_and_save_sampled_populations(
            df,
            nev_df, 
            year,
            datadir
            )
        