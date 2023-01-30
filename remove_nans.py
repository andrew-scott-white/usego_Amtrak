#!/home/emfreese/anaconda3/envs/grid_mod/bin/python
#SBATCH --time=12:00:00
#SBATCH --mem=MaxMemPerNode
#SBATCH --cpus-per-task=32s


import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
import datetime
import pandas as pd


# Run this to remove the nans from any inventories to ensure they work with HEMCO. There are plot tests below to check that the new dataset doesn't mess up any of the data.


#import dataset
#change run_name to your run's name
run_name = 'base'
data_path = f'../annual_emissions/inventory_power_plants_{run_name}.nc'

ds = xr.open_dataset(data_path)

ds_mod = ds.fillna(0)


#choose a pollutant to look at as our test
poll = 'NO'


#check that there are no longer nans in the new dataset
np.unique(np.isnan(ds_mod.isel(time = slice(-48,-24))[poll]).values)


#compare to the old dataset (should have True and False)
np.unique(np.isnan(ds.isel(time = slice(-48,-24))[poll]).values)

#check that values stay the same between the two:
print((ds.isel(time = slice(-50,-24))[poll]).values.ravel()- (ds_mod.isel(time = slice(-50,-24))[poll]).values.ravel())

#save out our dataset (you may have to delete the old one first)
ds_mod.to_netcdf(data_path, mode = 'w')



