#!/home/emfreese/anaconda3/envs/grid_mod/bin/python
#SBATCH --time=12:00:00
#SBATCH --mem=0
#SBATCH --cpus-per-task=32


import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
import datetime
import pandas as pd
import netCDF4 



#import dataset
#change run_name to your run's name
#run_name = 'base'
run_name = 'amtk_current'
year = 2015
#data_path = f'./annual_emissions/inventory_power_plants_scaled_{run_name}.nc'
#data_path = f'./annual_emissions/inventory_power_plants_{run_name}.nc'
path = f'~/fs11/amtrak_emissions/'
file_name = f'inventory_power_plants_{year}_scaled_{run_name}.nc'
data_path = path + file_name
#ds = xr.open_dataset(data_path)
ds = xr.open_dataset(data_path, engine='netcdf4',chunks={'time': 100})  # or chunk on spatial dims
ds = ds.astype('float32')

ds_mod = ds.fillna(0)


#choose a pollutant to look at as our test
poll = 'NO'

#check that there are no longer nans in the new dataset
np.unique(np.isnan(ds_mod.isel(time = slice(-48,-24))[poll]).values)


#compare to the old dataset (should have True and False)
np.unique(np.isnan(ds.isel(time = slice(-48,-24))[poll]).values)

#check that values stay the same between the two:
print((ds.isel(time = slice(-50,-24))[poll]).values.ravel()- (ds_mod.isel(time = slice(-50,-24))[poll]).values.ravel())

#data_path_new = f'./annual_emissions/inventory_power_plants_scaled_{run_name}_noNaN_test.nc'
data_path_new = path + f'inventory_power_plants_{year}_scaled_{run_name}_noNaN.nc'
#save out our dataset (you may have to delete the old one first)
ds_mod.to_netcdf(data_path_new, mode='w', compute=True)

    
