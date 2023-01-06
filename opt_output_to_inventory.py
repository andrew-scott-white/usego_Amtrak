#!/home/emfreese/anaconda3/envs/grid_mod/bin/python
#SBATCH --time=12:00:00
#SBATCH --cpus-per-task=12
#SBATCH -p edr



import xarray as xr
import numpy as np
import pandas as pd
import feather, h5py, sys, pickle
from timezonefinder import TimezoneFinder
from pytz import timezone
import pytz, os, datetime, netCDF4


# Get power model output and merge with power plant characteristics
gen = feather.read_dataframe(f'./outputs/gen_LA_no_ONG.feather')
carac = pd.read_csv(f'./good_model_inputs/inputs_gen_LA_no_ONG.csv')
print('data loaded')
# To match region totals with eGRID
#region_tots = feather.read_dataframe('egrid_corr_fac.feather').set_index('index')
#carac['NOX corr'] = carac['SUBRGN'].apply(lambda x: region_tots['NOX corr fac'][x])
#carac['SO2 corr'] = carac['SUBRGN'].apply(lambda x: region_tots['SO2 corr fac'][x])

# Clean columns name
carac = carac.drop('Unnamed: 0', axis=1)

# Merge
opt_out = pd.concat((carac,gen), axis=1)

# Add the missing final hour
opt_out['2016_365_23'] = opt_out['2016_365_22'].copy()

# Compute grid indices of all generators on NEI grid
startlat = 20.05
startlon = -139.95
step = 0.1
opt_out['idxLat'] = np.floor((opt_out['LAT'].values - startlat)/step).astype(int)
opt_out['idxLon'] = np.floor((opt_out['LON'].values - startlon)/step).astype(int)

#Check that all values are within grid limits
if (opt_out['idxLat'] < 0).sum() + (opt_out['idxLat'] > 400).sum() +\
	(opt_out['idxLon'] < 0).sum() + (opt_out['idxLon'] > 900).sum() != 0:
	raise ValueError('Some generators are outside grid limits')

    
def process_emissions(species_abbrev, NO = False, NO2 = False):
    """
    Process emissions of NO, NO2, SO2, CH4 and CO2 by multiplying the emissions factor times the generation (output in kg/s)
    initial data in: $MWH/3600sec -> MW/s -> * kg/MW -> kg/s$
    """
    emis_df = pd.DataFrame(data=opt_out[f'PL{species_abbrev}RTA'].values.reshape(len(opt_out.index),1) * opt_out[[f'2016_{day}_{hour}' for day in range(1,366) for hour in range(24)]] / 3600)
    emis_df['idxLat'] = opt_out['idxLat'].astype(int)
    emis_df['idxLon'] = opt_out['idxLon'].astype(int)
    emis_df['StateName'] = opt_out['StateName']
    if NO == True:
        mult = 0.8544304 # NO/NOx as estimated from NEI2011 inventory
    elif NO2 == True:
        mult = 1 - 0.8544304 # NO2/NOx as estimated from NEI2011 inventory
    else:
        mult = 1 # multiply by one to keep the same value if not NO or NO2
    emis_df[[f'2016_{day}_{hour}' for day in range(1,366) for hour in range(24)]] *= mult
    return(emis_df)

# Process emissions for each species
no = process_emissions(species_abbrev = 'NOX', NO = True)
print('no processed')

no2 = process_emissions(species_abbrev = 'NOX', NO2 = True)
print('no2 processed')

so2 = process_emissions(species_abbrev = 'SO2')
print('so2 processed')

# Integrate surface processing
path = '../annual_emissions/na_surfaces_01.mat'
arrays = {}
with h5py.File(path, 'r') as file:
	for k,v in file.items():
		arrays[k] = np.array(v)
		
areas = arrays['areas'] # these are the surfaces along one longitude band, need to repeat
# the values for all longitudes
surfaces = np.ones((400,900))
surfaces *= np.transpose(areas)

utc = pytz.utc
tf = TimezoneFinder(in_memory=True)

def offset(lat, lon):
	"""
	returns a location's time zone offset from UTC in minutes.
	"""
	today = datetime.datetime.now()
	tz_target = timezone(tf.certain_timezone_at(lat=lat, lng=lon))
	# ATTENTION: tz_target could be None! handle error case

	# today_target = tz_target.localize(today)
	# today_utc = utc.localize(today)
	# offset = today_utc - today_target
	offset = tz_target.utcoffset(today)

	# if `today` is in summer time while the target isn't, you may want to substract the DST
	offset -= tz_target.dst(today)
	return int(offset.total_seconds() / 3600)

# Grid emissions
# Need to shift the hours to account for time zone
myno = np.zeros((8760,400,900))
myno2 = np.zeros((8760,400,900))
myso2 = np.zeros((8760,400,900))


# Shift the hours to have everything at UTC time for GEOS-Chem
for idx in no.index:
    ofs = -offset(opt_out.loc[idx,'LAT'],opt_out.loc[idx,'LON'])
    myno[:, no.loc[idx, 'idxLat'], no.loc[idx, 'idxLon']] += np.roll(no.iloc[idx,:8760],ofs).astype(float)
    myno2[:, no2.loc[idx, 'idxLat'], no2.loc[idx, 'idxLon']] += np.roll(no2.iloc[idx,:8760],ofs).astype(float)
    myso2[:, so2.loc[idx, 'idxLat'], so2.loc[idx, 'idxLon']] += np.roll(so2.iloc[idx,:8760],ofs).astype(float)

myno = myno/surfaces # nox in kg/m2/s
myno2 = myno2/surfaces
myso2 = myso2/surfaces


# Load mask of us states
with open('mask_us_neigrid.pkl', 'rb') as f:
    mask = pickle.load(f)

# Add power plant emissions outside of the US
for day in range(1,366):
    # Load NEI values for that day
    mth = pd.to_datetime(f'2011+{day}', format='%Y+%j').strftime(format='%m')
    dat = pd.to_datetime(f'2011+{day}', format='%Y+%j').strftime(format='%Y%m%d')
    nei1 = xr.open_dataset(f'/net/geoschem/data/gcgrid/data/ExtData/HEMCO/NEI2011/v2015-03/{mth}/NEI11_0.1x0.1_{dat}_egu.nc')
    nei2 = xr.open_dataset(f'/net/geoschem/data/gcgrid/data/ExtData/HEMCO/NEI2011/v2015-03/{mth}/NEI11_0.1x0.1_{dat}_egupk.nc')
    for t in range(24):
        # where there are no US states, copy the values ffrom NEI
        myno[(day-1)*24+t,:,:][mask == -1] = nei1['NO'].isel(time=t, lev=0).values[mask == -1] +\
                                                nei2['NO'].isel(time=t, lev=0).values[mask == -1]
        myno2[(day-1)*24+t,:,:][mask == -1] = nei1['NO2'].isel(time=t, lev=0).values[mask == -1] +\
                                                nei2['NO2'].isel(time=t, lev=0).values[mask == -1]
        myso2[(day-1)*24+t,:,:][mask == -1] = nei1['SO2'].isel(time=t, lev=0).values[mask == -1] +\
                                                nei2['SO2'].isel(time=t, lev=0).values[mask == -1]

# Initialize dataset dimensions and create the dataarrays
#################### Push final output ###########################################

lat = np.arange(20.05, 60, 0.1)
lon = np.arange(-139.95, -49.95, 0.1)

# Build new netCDF file
f_out = f'../annual_emissions/inventory_power_plants_LA_no_ONG.nc'
print('nc file made')
if os.path.isfile(f_out):
	#print('Clobbering ' + f_out)
	os.remove(f_out)
nc_out = netCDF4.Dataset(f_out,'w',format='NETCDF4')
tdy = datetime.datetime.now().strftime(format='%Y-%m-%d')

# Global attributes
nc_out.Title = 'Hourly CH power plant inventory'
nc_out.Conventions = 'COARDS'
nc_out.History = f'Created {tdy}',
nc_out.Contact = 'emfreese@mit.edu'
dim_dict = {'time': {'units': 'hours since 2016-01-01 00:00:00',
					 'dtype': 'i4',
					 'data':  list(range(8760)),
					 'other': {'calendar': 'gregorian'}},
			'lat':  {'units': 'degrees_north',
					 'dtype': 'f8',
					 'data':  lat,
					 'other': {'long_name': 'latitude'}},
			'lon':  {'units': 'degrees_east',
					 'dtype': 'f8',
					 'data':  lon,
					 'other': {'long_name': 'longitude'}}}

for dim_name, dim_data in dim_dict.items():
	nc_out.createDimension(dim_name,len(dim_data['data']))
	var_temp = nc_out.createVariable(dim_name,dim_data['dtype'],(dim_name))
	var_temp[:] = dim_data['data'][:],
	var_temp.units = dim_data['units']
	for att_name, att_val in dim_data['other'].items():
		var_temp.setncattr(att_name,att_val)

chunks = [1,len(lat),len(lon)]
var_list = ['NO','NO2','SO2']
trad = {'NO': myno, 'NO2': myno2, 'SO2': myso2}

for var in var_list:
    nc_var = nc_out.createVariable(var,'f8',(('time','lat','lon')),
									 chunksizes=chunks,complevel=1,zlib=True)
    nc_var.units = 'kg/s/m2'
    nc_var.long_name = f'US {var} power plant emissions from my optimization model'
    src_data = trad[var]
    
    for i_time in range(8760):
        var_data = src_data[i_time,:,:]
        nc_var[i_time,:,:] = var_data.copy()

nc_out.close()
