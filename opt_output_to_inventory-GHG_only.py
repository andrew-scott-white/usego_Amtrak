#!/home/gchossie/anaconda3/envs/evchina/bin/python

#SBATCH --time=12:00:00
#SBATCH --mem=100G
#SBATCH --cpus-per-task=1


import xarray as xr
import numpy as np
import pandas as pd
import feather, h5py, sys, pickle
from timezonefinder import TimezoneFinder
from pytz import timezone
import pytz, os, datetime, netCDF4


# Get power model output and merge with power plant characteristics
gen = feather.read_dataframe(f'./outputs/gen_{run_name}.feather')
carac = pd.read_csv(f'./good_model_inputs/inputs_gen_{run_name}.csv')

# To match region totals with eGRID unhash this
#region_tots = feather.read_dataframe('egrid_corr_fac.feather').set_index('index')
#carac['NOX corr'] = carac['SUBRGN'].apply(lambda x: region_tots['NOX corr fac'][x])
#carac['SO2 corr'] = carac['SUBRGN'].apply(lambda x: region_tots['SO2 corr fac'][x])

# Clean columns name
carac = carac.drop('Unnamed: 0', axis=1)

# Merge
opt_out = pd.concat((carac,gen), axis=1)

# Add the missing final hour
opt_out['2017_365_23'] = opt_out['2017_365_22'].copy()

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

co2 = pd.DataFrame(data=opt_out['PLCO2RTA'].values.reshape(len(opt_out.index),1) * opt_out[[f'2017_{day}_{hour}' for day in range(1,366) for hour in range(24)]] / 3600)
# so2 is now in lb/s
co2['idxLat'] = opt_out['idxLat'].astype(int)
co2['idxLon'] = opt_out['idxLon'].astype(int)
co2['StateName'] = opt_out['StateName']
#so2[[f'2017_{day}_{hour}' for day in range(1,366) for hour in range(24)]] *= mult
# co2 is in kg/s

ch4 = pd.DataFrame(data=opt_out['PLCH4RTA'].values.reshape(len(opt_out.index),1) * opt_out[[f'2017_{day}_{hour}' for day in range(1,366) for hour in range(24)]] / 3600)
# so2 is now in lb/s
ch4['idxLat'] = opt_out['idxLat'].astype(int)
ch4['idxLon'] = opt_out['idxLon'].astype(int)
ch4['StateName'] = opt_out['StateName']
#ch4[[f'2017_{day}_{hour}' for day in range(1,366) for hour in range(24)]] *= mult
# ch4 is in kg/s

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

myco2 = np.zeros((8760,400,900))
mych4 = np.zeros((8760,400,900))

# Shift the hours to have everything at UTC time for GEOS-Chem
for idx in co2.index:
    ofs = -offset(opt_out.loc[idx,'LAT'],opt_out.loc[idx,'LON'])
    myco2[:, co2.loc[idx, 'idxLat'], co2.loc[idx, 'idxLon']] += np.roll(co2.iloc[idx,:8760],ofs).astype(float)
    mych4[:, ch4.loc[idx, 'idxLat'], ch4.loc[idx, 'idxLon']] += np.roll(ch4.iloc[idx,:8760],ofs).astype(float)

myco2 = myco2/surfaces
mych4 = mych4/surfaces

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
        myco2[(day-1)*24+t,:,:][mask == -1] = nei1['CO2'].isel(time=t, lev=0).values[mask == -1] +\
                                                nei2['CO2'].isel(time=t, lev=0).values[mask == -1]
        mych4[(day-1)*24+t,:,:][mask == -1] = nei1['CH4'].isel(time=t, lev=0).values[mask == -1] +\
                                                nei2['CH4'].isel(time=t, lev=0).values[mask == -1]
# Initialize dataset dimensions and create the dataarrays
#################### Push final output ###########################################

lat = np.arange(20.05, 60, 0.1)
lon = np.arange(-139.95, -49.95, 0.1)

# Build new netCDF file
f_out = f'../annual_emissions/inventory_power_plants_{run_name}_ghg.nc'

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
dim_dict = {'time': {'units': 'hours since 2017-01-01 00:00:00',
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
var_list = ['CO2','CH4']
trad = {'CO2': myco2, 'CH4':mych4}

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