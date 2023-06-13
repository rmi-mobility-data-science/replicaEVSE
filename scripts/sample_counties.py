import pandas as pd
import numpy as np
import replicaEVSE.load_curve as sim
import replicaEVSE.datautils as simdu
import os

datadir = '../../data/'
mode = 'PRIVATE_AUTO'

# read in the joined trips and population data sets
merged_df = pd.read_parquet(os.path.join(datadir, 'wa_pop_and_trips_sorted_county.parquet'))

# right now, only look at private auto trips
df = merged_df.loc[merged_df['mode'] == mode]
# take out the mobile and commercial MHDV

### TODO: revisit taking out mobile home owners
df = df[(df['building_type'] != 'mobile') & (df['building_type'] != None)]


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

personal = ['Personal Sedan', 'Personal Crossover', 'Personal Truck/SUV']
commercial = ['Commercial Sedan', 'Commercial Crossover', 'Commercial Truck/SUV']

# get the stock rollover data
stock_rollover = pd.read_csv(datadir+'ldv_population_output_adjusted.csv')
ev_cond = stock_rollover['Powertrain'].isin(['EV', 'PHEV']).reset_index(drop=True)
nev_df = stock_rollover[ev_cond].copy()
nev_df = nev_df[nev_df['domicile'] != 'other'].copy()
nev_df.drop(columns=['Unnamed: 0'], inplace=True)

for year in np.arange(2022, 2036, 1):
    segment_persons = []
    for segment in personal+commercial:
        seg_string = segment.replace(" ", "_").lower().replace("/suv", "")
        simulation_id = f'{seg_string}_{str(year)}'
        print(simulation_id)
        
        eff = segment_efficiency(segment)

        print(eff)
        nev_df = stock_rollover[stock_rollover['Powertrain']=='EV'].copy()
        nev_df_seg = nev_df[nev_df['Vehicle_type']==segment].copy()
        
        
        # we dont want to include the same people in multiple segments.
        # this results in not enough people in some small counties...
        #if len(segment_persons) == 0:
        #    df_county_subset = simdu.sample_people_by_county(df, nev_df, year=year) 
        #else:
        #    df = df[~df['person_id'].isin(segment_persons)]    
        # ~3 mins
        df_county_subset = simdu.sample_people_by_county(df, nev_df, year=year)
        df_county_subset['efficiency'] = eff
        segment_persons.extend(df_county_subset['person_id'].tolist())
        # ddf = dd.from_pandas(df_county_subset, npartitions=500)
        df_county_subset.to_parquet(os.path.join(datadir, f'county_sample_{simulation_id}.parquet'), 
                                    engine='pyarrow',
                                    compression='snappy')