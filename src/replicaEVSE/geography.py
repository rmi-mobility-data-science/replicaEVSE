import os
import geopandas as gpd
import requests



def get_bg_shapefiles(state, counties=None):
    """Downloads zipped shapefile of census block groups and converts it into a geodataframe.
    Shapefiles for geographies other than census tracts can be found at: https://www2.census.gov/geo/tiger/TIGER2021/

    Census block groups are defined here: https://en.wikipedia.org/wiki/Census_block_group
    
    Parameters
    ----------
    state : str
        Two-digit numerical FIPS state code
        https://en.wikipedia.org/wiki/Federal_Information_Processing_Standard_state_code
    
    Returns
    -------
    geodataframe
        shapes associated with census tracts in a given state
    """
    #Note: the year of the blockgroups is important here. It looks like the blockgroups 
    # Replica uses are from 2019. The blockgroups change in 2020.
    # Note, this may have changed since John wrote this. 
    fname = '../../data/blockgroups_'+state+'.zip'
    if not os.path.exists(fname):
        url = 'https://www2.census.gov/geo/tiger/TIGER2019/BG/tl_2019_'+state+'_bg.zip'
        response = requests.get(url, timeout=10)
        # write the file to disk
        open(fname, 'wb').write(response.content)
    gdf = gpd.read_file(fname)
    
    # to select specific counties
    if counties is not None:
        if isinstance(counties) is not list:
            counties = [counties]
            gdf = gdf.loc[gdf.COUNTYFP.isin(counties)]
        return gdf
    else:
        return gdf