"""This scirpt adds dwell time to the data
assuming only private auto mode (ldvs). It should be run 
after merging the counties (merge_counties.py)."""

import pandas as pd
import os
import replicaEVSE.datautils as datautils

pd.set_option('display.max_columns', None)

mode = 'PRIVATE_AUTO'
datadir = '../../data/'

# read in data and filter for mode
merged_df = pd.read_parquet(os.path.join(datadir, 'wa_pop_and_trips_sorted_county.parquet'))
df = merged_df.loc[merged_df['mode'] == mode]

# instead of looping, use groupby on person and weekday
# and apply our function to calculate dwell time to each person and 
# weekday group. This ensures we calculate the overnight dwell time
# for each person and weekday. >2 hours
groupby_df_stop_dur = df.groupby(['person_id', 'weekday']).apply(datautils.calculate_stop_duration).reset_index(drop=True)

# save the file
groupby_df_stop_dur.to_parquet(datadir+'wa_ldv_trips_with_county_and_dwell_time.parquet')