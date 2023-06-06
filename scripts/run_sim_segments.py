import pandas as pd
import numpy as np
import replicaEVSE.load_curve as sim
import replicaEVSE.datautils as simdu
import os
import joblib
from tqdm import tqdm

seg_list = ['Personal Sedan',
 'Personal Crossover',
 'Personal Truck/SUV',
 'Commercial Sedan',
 'Commercial Crossover',
 'Commercial Truck/SUV']

datadir = '../../data/'

existing_load=pd.read_csv(datadir+'EIA_demand_summary.csv') 

charge_df_list = []
loads_df_list = []
for year in [2023, 2025, 2030, 2035]:
    charge_df_seg_list = []
    loads_df_seg_list = []
    for segment in seg_list:
        seg_string = segment.replace(" ", "_").lower().replace("/suv", "")
        simulation_id = f'{seg_string}_{str(year)}'
        df_county_subset = pd.read_parquet(os.path.join(datadir, f'county_sample_{simulation_id}.parquet'))

        number_of_chunks = 10000
        df_list = np.array_split(df_county_subset, number_of_chunks)

        charge_sims = joblib.Parallel(verbose=10, n_jobs=-1)(joblib.delayed(sim.simulate_person_load)(
        trips_df=df_i, 
        existing_load=existing_load,
        simulation_id=simulation_id,
        managed=False,
        efficiency=df_i['efficiency'].values[0],
        frac_work_charging=0.2,
        frac_non_office_charging=0.1,
        frac_civic_charging=0.5,
        frac_multiunit_charging=0.2,
        frac_singleunit_charging=1.0,
        frac_public_dcfc=1.0) 
        for df_i in df_list)

        print('creating charge and loads')
        charges_list = [x['charges'] for x in charge_sims]
        loads_list = [x['loads'] for x in charge_sims]

        # restack the dataframes
        charges_df = pd.concat(charges_list)
        loads_df = pd.concat(loads_list) # huge ~200 million rows
        
        charges_df['year'] = year
        charges_df['segment'] = segment
        charges_df['work_frac'] = 0.2
        charges_df['multiunit_frac'] = 0.2

        loads_df['year'] = year
        loads_df['segment'] = segment
        loads_df['work_frac'] = 0.2
        loads_df['multiunit_frac'] = 0.2

        charges_df.to_parquet(os.path.join(datadir, f'charges_{year}_{seg_string}_2023-06-05.parquet'))
        loads_df.to_parquet(os.path.join(datadir, f'loads_{year}_{seg_string}_2023-06-05.parquet'))


"""tot_charge_df = pd.concat(charge_df_list)
tot_load_df = pd.concat(loads_df_list)

tot_charge_df.to_parquet(os.path.join(datadir, f'charges_{year}_total_2023-06-05.parquet'))
tot_load_df.to_parquet(os.path.join(datadir, f'loads_{year}_total_2023-06-05.parquet'))"""
