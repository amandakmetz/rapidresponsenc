
"""
download population, race and ethnicity variables 
from 2010 decennial census and 2019 5yr acs

prerequisites: county fips crosswalk for state
    
"""

import cenpy as c
import pandas as pd
import geopandas as gpd

##### SETUP
state_name = 'NC'
state_fips = '37'

# get county crosswalk for state
state_cross = pd.read_csv('./county_fips/{0}_county_cross.csv'.format(state_name.lower()))
state_cross['fips'] = state_cross['fips'].map(lambda x: str(x).zfill(3)) 

# specify counties
state_counties = list(state_cross['fips'])

# set census API key
c.set_sitekey('APIKEY', overwrite=True)

##### ---------------------------------------------------CENSUS BLOCKS, DEC 10

# specify block shapefile if needed
block_file = './NC/tl_2020_37_tabblock10/tl_2020_37_tabblock10.shp'

# specify output files
output_csv = './NC/Census/nc_blocks_dec10.csv'
output_shp = './NC/Shapefiles/nc_blocks_dec10.shp'

##### GET DATA

# check out all codes available
codes = c.explorer.available()

DEC_codes = codes.loc[codes.index.str.contains('DECENNIAL')]['title']

# explain dataset
datasets = list(c.explorer.available(verbose=True).items())
pd.DataFrame(datasets).head()
dataset = 'DECENNIALSF12010'
c.explorer.explain(dataset)

# set connection to survey / year
conn = c.remote.APIConnection('DECENNIALSF12010')

# check out all variables in current connection
test = conn.geographies
test = conn.geographies['fips'].head(100)

var = conn.variables
print('Number of variables in', dataset, ':', len(var))
conn.variables.head()

# specify variables for decennial census
census_vars = ['P005001','P005003','P005004','P005005','P005006','P005010',
               'P011001','P011005','P011006','P011007','P011008','P011002']

# set empty dataframe to fill
data = pd.DataFrame(columns=census_vars + ['state', 'county', 'tract', 'block', 'geoid'])

# make census requests and store information in dataframe
for county in state_counties:
    print(county)
    county_data = conn.query(cols=census_vars, geo_unit='block', geo_filter={'state':state_fips, 'county':county})
    data = data.append(county_data)
    
# create geoid column and rename vars
data['geoid'] = data['state'] + data['county'] + data['tract'] + data['block']

data[census_vars] = data[census_vars].astype(float)

data = data.rename(columns = {
                            'P005001':'tot',
                            'P005003':'NHwhite',
                            'P005004':'NHblack',
                            'P005005':'NHnat',
                            'P005006':'NHasi',
                            'P005010':'hispanic',
                            'P011001':'totVAP',
                            'P011005':'WVAP',
                            'P011006':'BVAP',
                            'P011007':'NatVAP',
                            'P011008':'AVAP',
                            'P011002':'HVAP'})

# write csv to file
data.to_csv(output_csv)

#### IF SHAPEFILES ARE NEEDED - GET TIGER DATA

# check available tiger datasets
c.tiger.available()

# set map service
conn.set_mapservice('tigerWMS_Census2010')

# available map layers - layer 18 is blocks
conn.mapservice.layers
conn.mapservice.layers[18]

# query all available blocks
geodata = gpd.GeoDataFrame()

# grabbing blocks from every county
for county in state_counties:
    print(county)
    county_geodata = conn.mapservice.query(layer=18, where=('STATE={0} AND COUNTY={1}'.format(state_fips, county)))
    geodata = geodata.append(county_geodata)
    
# blocks from entire state - doesn't work because of 100000 block limit
# this will work for higher level geographies
#geodata = conn.mapservice.query(layer=18, where='STATE=37')

# preview geodata
geodata.iloc[:5, :5]
geodata.dtypes
geodata['GEOID'].head(10)
data['geoid'].head(10)

joined_data = geodata.merge(data, left_on='GEOID', right_on='geoid', how='left')
joined_data.iloc[:5, -5:]

# rename columns and drop 
joined_data.drop(columns=['geoid', 'state', 'county', 'tract', 'block'], inplace=True)

# save shapefile
joined_data.to_file(output_shp)


#### IF USING EXISTING SHAPEFILE - JOIN TO GEOMETRY

# downloads are limited to 100000 blocks per request

# read in blocks
blocks = gpd.read_file(block_file)

blocks.dtypes
blocks['GEOID10'].head()
data['geoid'].head()

# check for same number of blocks in both
print(len(blocks) == len(data))

# merge shp and df
joined_blocks = blocks.merge(data, left_on='GEOID10', right_on='geoid',how='left')
joined_blocks.dtypes

#write to file    
joined_blocks.to_file(output_shp)


##### ---------------------------------------------------BLOCK GROUPS, ACS 19

# specify block group shapefile if needed
#bg_file = './NC/tl_2019_37_bg/tl_2019_37_bg.shp'

# specify output files
output_csv = './NC/Census/nc_bg_acs19.csv'
output_shp = './NC/Shapefiles/nc_bg_acs19.shp'

##### GET DATA

# check out ACS codes available
codes = c.explorer.available()

# check out codes for ACS 5Yr 2019 only
ACScodes = codes.loc[codes.index.str.contains('5Y2019')]['title']

# explain dataset
datasets = list(c.explorer.available(verbose=True).items())
pd.DataFrame(datasets).head()
dataset = 'ACSDT5Y2019'
c.explorer.explain(dataset)

# set connection to survey / year
conn = c.remote.APIConnection('ACSDT5Y2019')

# check out the geographic filter requirements
test = conn.geographies
test = conn.geographies['fips'].head(100)

# check out all variables in current ACS connection
var = conn.variables
print('Number of variables in', dataset, ':', len(var))
conn.variables.head()

# geo_unit and geo_filter are both necessary arguments for the query() function. 
# geo_unit specifies the scale at which data should be taken. geo_filter then 
# creates a filter to ensure too much data is not downloaded. 

g_unit = 'block group:*'
g_filter = {'state':'37',
            'county':'*',
            'tract':'*'}

# find list of variables in relevant table
cols = list(conn.varslike('B03002').index)
cols.extend(['NAME', 'GEO_ID'])

# #set empty dataframe to fill
# data = pd.DataFrame(columns=census_vars + ['state', 'county', 'tract', 'blockgroup'])

# query current ACS connection with columns, filter and geofilter settings
data = conn.query(cols, geo_unit=g_unit, geo_filter=g_filter)

data.index = data.GEO_ID
data.iloc[:5, -5:]

# give data new geoid column to match
data.head()
data['geoid'] = data.index.map(lambda x:x.replace('1500000US', ''))

data[cols] = data[cols].astype(float)

data = data.rename(columns = {
                            'P005001':'tot',
                            'P005003':'NHwhite',
                            'P005004':'NHblack',
                            'P005005':'NHnat',
                            'P005006':'NHasi',
                            'P005010':'hispanic',
                            'P011001':'totVAP',
                            'P011005':'WVAP',
                            'P011006':'BVAP',
                            'P011007':'NatVAP',
                            'P011008':'AVAP',
                            'P011002':'HVAP'})

# write csv to file
data.to_csv(output_csv)

#### IF SHAPEFILES ARE NEEDED - GET TIGER DATA

# check available tiger datasets
c.tiger.available()

conn.set_mapservice('tigerWMS_ACS2019')

# available map layers
conn.mapservice.layers
conn.mapservice.layers[10]

# query all available block groups (layer 10) for NC
geodata = gpd.GeoDataFrame()

geodata = conn.mapservice.query(layer=10, where='STATE={0}'.format(state_fips))

# preview geodata
geodata.iloc[:5, :5]
geodata.dtypes


joined_data = geodata.merge(data, left_on='GEOID', right_on='geoid', how='left')
joined_data.iloc[:5, -5:]
joined_data.dtypes

# rename columns and drop  - table B03002
joined_data.dtypes
joined_data = joined_data.rename(columns = {
                            'B03002_001E':'tot19',
                            'B03002_003E':'NHwhite19',
                            'B03002_004E':'NHblack19',
                            'B03002_005E':'NHnat19',
                            'B03002_006E':'NHasi19',
                            'B03002_012E':'hispanic19',
                            'NAME_x':'NAME'})

joined_data.drop(columns=['B03002_021E', 'B03002_020E', 'B03002_002E', 
                          'B03002_009E', 'B03002_007E', 'B03002_008E',
                          'B03002_013E', 'B03002_011E', 'B03002_010E',
                          'B03002_017E', 'B03002_016E', 'B03002_015E', 
                          'B03002_014E', 'B03002_018E', 'B03002_019E',
                          'NAME_y', 'state', 'county', 'tract', 'block group', 
                          'geoid'], inplace=True)

# save to file
joined_data.to_file(output_shp)

#### IF USING EXISTING SHAPEFILE - JOIN TO GEOMETRY

# downloads are limited to 100000 blocks

# read in blocks
blocks = gpd.read_file(block_file)
blocks.dtypes
blocks['GEOID10'].head()
data['geoid'].head()

# check for same number of blocks in both
print(len(blocks) == len(data))

# merge shp and df
joined_blocks = blocks.merge(data, left_on='GEOID10', right_on='geoid',how='left')
joined_blocks.dtypes

#write to file    
joined_blocks.to_file(output_shp)

