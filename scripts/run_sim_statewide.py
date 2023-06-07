import pandas as pd
import numpy as np
import replicaEVSE.load_curve as sim
import replicaEVSE.datautils as simdu
import os
import joblib
import dask.dataframe as dd


pd.set_option('display.max_columns', None)

datadir = '../../data/'
mode = 'PRIVATE_AUTO'
test = False



#Created in the EIA_data_download.ipynb notebook
existing_load=pd.read_csv(datadir+'EIA_demand_summary.csv') 
if test:
    # df = pd.read_parquet(os.path.join(datadir, 'wa_pop_and_trips_subsample.parquet'))
    df = pd.read_parquet(os.path.join(datadir, 'wa_pop_and_trips_sorted_county.parquet'))
    df = df.head(10000)
    df = df.loc[df['mode'] == mode]
    simulation_id = 'dev'

else: 
    # read in the joined trips and population data sets
    merged_df = pd.read_parquet(os.path.join(datadir, 'wa_pop_and_trips_sorted_county.parquet'))

    # right now, only look at private auto trips
    df = merged_df.loc[merged_df['mode'] == mode]
    # take out the mobile and commercial MHDV

df = df[(df['building_type'] != 'mobile') & (df['building_type'] != None)]



stock_rollover = pd.read_csv(datadir+'LDV_pop_adjusted.csv')
efficiency = pd.read_csv(datadir+'vehicle_inputs.csv')

nev_df = stock_rollover[stock_rollover['Powertrain']=='EV'].copy()
# nev_df = nev_df[nev_df['Vehicle_type']==segment].copy()
nev_df = nev_df[nev_df['domicile'] != 'other'].copy()


reduced_df = []
unique_df = df.drop_duplicates(subset=['person_id'])[['person_id', 'destination_county', 'building_type']]
for year in [2023, 2025, 2030, 2035]:
    print(nev_df[str(year)].sum())
    num_to_select = nev_df[str(year)].sum()
    selected = unique_df.person_id.sample(n=num_to_select, replace=False, random_state=42)
    
    # grab only those selected people from the original dataframe
    year_df = df[(df['person_id'].isin(selected))].copy()
    year_df['year'] = year
    reduced_df.append(year_df)
    
final_df = pd.concat(reduced_df)

seg_list = ['Personal Sedan',
 'Personal Crossover',
 'Personal Truck/SUV',
 'Commercial Sedan',
 'Commercial Crossover',
 'Commercial Truck/SUV']

datadir = '../../data/'

existing_load=pd.read_csv(datadir+'EIA_demand_summary.csv') 
simulation_id = f'by_year_{str(year)}'
charge_df_list = []
loads_df_list = []
workfrac_arr = np.linspace(0.2, 0.6, 4)
multiunitfrac_arr = np.linspace(0.2, 0.6, 4)
years = [2023, 2025, 2030, 2035]

for year, workfrac, multiunitfrac  in zip(years, workfrac_arr, multiunitfrac_arr):
    charge_df_seg_list = []
    loads_df_seg_list = []
    df_year = final_df[final_df['year'] == year]

    number_of_chunks = 10000
    df_list = np.array_split(df_year, number_of_chunks)

    charge_sims = joblib.Parallel(verbose=10, n_jobs=-1)(joblib.delayed(sim.simulate_person_load)(
    trips_df=df_i, 
    existing_load=existing_load,
    simulation_id=simulation_id,
    managed=False,
    efficiency=0.3,
    frac_work_charging=workfrac,
    frac_non_office_charging=0.1,
    frac_civic_charging=0.5,
    frac_multiunit_charging=multiunitfrac,
    frac_singleunit_charging=1.0,
    frac_public_dcfc=0.9) 
    for df_i in df_list)

    print('creating charge and loads')
    charges_list = [x['charges'] for x in charge_sims]
    loads_list = [x['loads'] for x in charge_sims]

    # restack the dataframes
    charges_df = pd.concat(charges_list)
    loads_df = pd.concat(loads_list) # huge ~200 million rows
    
    charges_df['year'] = year
    charges_df['segment'] = 'all'
    charges_df['work_frac'] = workfrac
    charges_df['multiunit_frac'] = multiunitfrac

    loads_df['year'] = year
    loads_df['segment'] = 'all'
    loads_df['work_frac'] = workfrac
    loads_df['multiunit_frac'] = multiunitfrac

    charges_df.to_parquet(os.path.join(datadir, f'loads_charges/charges_{year}_by_year_2023-06-06.parquet'))
    loads_df.to_parquet(os.path.join(datadir, f'loads_charges/loads_{year}_by_year_2023-06-06.parquet'))