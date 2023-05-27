import pandas as pd

datadir = '../../data/'

def merge_counties_to_full_df():
    import dask.dataframe as dd
    from dask.distributed import Client, LocalCluster
    cluster = LocalCluster(n_workers=64)
    client = Client(cluster)

    df = dd.read_parquet(datadir+'/wa_pop_and_trips_sorted.parquet') # len = 51727268
    # counties = dd.read_parquet(datadir+'/population_counties_dataset.parquet', engine='pyarrow')
    # cdf = dd.merge(df, counties, on='person_id', how='left')

    # change data types for consistency
    # cdf['home_cty'] = cdf['home_cty'].astype(str)
    # cdf['home_st'] = cdf['home_st'].astype(str)

    #cdf.to_parquet(datadir+'/wa_pop_and_trips_sorted_county.parquet', engine='pyarrow')

    #cdf = dd.read_parquet(datadir+'/wa_pop_and_trips_sorted_county.parquet', split_row_groups=True)
    # read in blockgroup info
    bg_df = dd.read_csv(datadir+'blockgroup_counties.csv')
    bg_df['destination_bgrp'] = bg_df.destination_bgrp.astype(str)
    bg_df['destination_county'] = bg_df.County.astype(str)

    merged_df = dd.merge(df, bg_df, on='destination_bgrp', how='left')

    merged_df['destination_county'] = merged_df['County'] + ', WA'

    merged_df.to_parquet(datadir+'wa_pop_and_trips_sorted_county_2.parquet')

    client.close()

def merge_counties_to_full_df_pandas():
    df = pd.read_parquet(datadir+'/wa_pop_and_trips_sorted.parquet') # len = 51727268

    print(len(df))
    # read in blockgroup info
    bg_df = pd.read_csv(datadir+'blockgroup_counties.csv')
    
    # there are duplicates in the blockgroups. Not sure why. Drop them.
    bg_df = bg_df.drop_duplicates(subset='destination_bgrp')
    bg_df['destination_bgrp'] = bg_df.destination_bgrp.astype(str)
    bg_df['destination_county'] = bg_df.County.astype(str)
    bg_df['destination_county'] = bg_df['County'] + ', WA'
    bg_df = bg_df[['destination_bgrp', 'destination_county']]

    print(len(bg_df))

    print('merging')    
    # merge the two
    merged_df = pd.merge(df, bg_df, on='destination_bgrp', how='left')

    print('saving')
    # save as a parquet file
    merged_df.to_parquet(datadir+'wa_pop_and_trips_sorted_county.parquet')


if __name__ == '__main__':
    merge_counties_to_full_df_pandas()
