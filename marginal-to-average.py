import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import Point
import matplotlib.pyplot as plt
import xarray as xr 
import dask
from datetime import datetime, timedelta

case_name = 'amtk_current'
base_load = pd.read_csv('~/us_ego/inputs/inputs_load_base.csv') # MWh
amtk_load = pd.read_csv(f'~/us_ego/inputs/inputs_load_{case_name}.csv') # MWh

# open existing file; amtrak load + base load grid emissions
path='~/us_ego/annual_emissions/'
name =f'inventory_power_plants_{case_name}.nc'
base_name = f'inventory_power_plants_base.nc'
amtrak_grid_emissions = xr.open_dataset(path+name, engine="netcdf4")
base_grid_emissions = xr.open_dataset(path+base_name, engine="netcdf4")

# Using Dask for large arrays
amtrak_grid_chunked = amtrak_grid_emissions.chunk({"time": 100})  
base_grid_chunked = base_grid_emissions.chunk({"time": 100})  

amtrak_grid = amtrak_grid_chunked.fillna(0)
base_grid = base_grid_chunked.fillna(0)

del_load = amtk_load - base_load # load attributable to Amtrak

# Marginal Amtrak emissions: calculate difference between amtk_grid and base_grid
#marginal_diff_grid = xr.ufuncs.subtract(amtrak_grid, base_grid) # ufuncs automatically uses dask chunks

# remove NaN
#marginal_diff_grid_noNAN = marginal_diff_grid.fillna(0)

# save to Dataset to path
#path='~/fs11/amtrak_emissions/'
#name = (f'inventory_power_plants_{case_name}_marginal.nc')
#marginal_diff_grid_noNAN.to_netcdf(path+name)

amtk_NO = np.asarray(amtrak_grid["NO"])
amtk_NO2 = np.asarray(amtrak_grid["NO2"])
amtk_SO2 = np.asarray(amtrak_grid["SO2"])

base_NO = np.asarray(base_grid["NO"])
base_NO2 = np.asarray(base_grid["NO2"])
base_SO2 = np.asarray(base_grid["SO2"])

start_date = datetime(2016,1,1,0,0,0)
dt = timedelta(hours=1)
date = start_date

def process_hour(tt):
    t_low = tt
    t_high = tt + 2

    if tt == 0:
        amtk_load_slice = amtk_load.query('t < @t_high')["demandLoad"]
        base_load_slice = base_load.query('t < @t_high')["demandLoad"] 

    elif tt == len(amtk_NO[:,0,0]):
        amtk_load_slice = amtk_load.query('@t_low < t')["demandLoad"]
        base_load_slice = base_load.query('@t_low < t')["demandLoad"] 
    else:
        amtk_load_slice = amtk_load.query('@t_low < t < @t_high')["demandLoad"]
        base_load_slice = base_load.query('@t_low < t < @t_high')["demandLoad"] 

    # sum hourly load for amtk and base
    amtk_load_slice_sum = round(np.sum(amtk_load_slice),2)
    base_load_slice_sum = round(np.sum(base_load_slice),2)

    if amtk_load_slice_sum + base_load_slice_sum == 0:
        return f"zero load index = {tt}"

    phi = (amtk_load_slice_sum-base_load_slice_sum)/(amtk_load_slice_sum)
    date = start_date + dt

    result = xr.ufuncs.multiply(amtrak_grid.sel(time=date), phi) # ufuncs automatically uses dask chunks
    return (tt, date, result)

for tt in range(len(amtk_NO[:,0,0])):
    if amtk_load.loc[[]]


    # save to Dataset to path
    path='~/fs11/amtrak_emissions/'
    name = (f'inventory_power_plants_{case_name}_average.nc')
    avg_diff_grid_noNAN.to_netcdf(path+name)

    print('Total base grid load',round(np.sum(base_load["demandLoad"])/1000/1000,2),'TWh/yr') # Mg/yr
    print('Total base+amtk grid load',round(np.sum(amtk_load["demandLoad"])/1000/1000,2),'TWh/yr') # Mg/yr

    print('Total NOx emissions from baseline:',(np.sum(base_NO) + np.sum(base_NO2))*3600*123210000/1000,'Mg/yr') # Mg/yr
    print('Total NOx emissions from baseline+Amtrak:',(np.sum(amtk_NO) + np.sum(amtk_NO2))*3600*123210000/1000,'Mg/yr') # Mg/yr

    print('Total SO2 emissions from baseline:',np.sum(base_SO2)*3600*123210000/1000,'Mg/yr') # Mg/yr
    print('Total SO2 emissions from baseline+Amtrak:',np.sum(amtk_SO2)*3600*123210000/1000,'Mg/yr') # Mg/yr

    #print('Annual marginal NOx emissions from Amtrak:',(np.sum(amtk_NO) + np.sum(amtk_NO2) - np.sum(base_NO) - np.sum(base_NO2))*3600*123210000/1000,'Mg/yr') # Mg/yr
    #print('Annual grid average NOx emissions from Amtrak:',(np.sum(avg_diff_grid_noNAN["NO"].values)+np.sum(avg_diff_grid_noNAN["NO2"].values))*3600*123210000/1000,'Mg/yr') # Mg/yr

    #print('Annual marginal SO2 emissions from Amtrak:',(np.sum(amtk_SO2)-np.sum(base_SO2))*3600*123210000/1000,'Mg/yr') # Mg/yr
    #print('Annual grid average SO2 emissions from Amtrak:',(np.sum(avg_diff_grid_noNAN["SO2"].values))*3600*123210000/1000,'Mg/yr') # Mg/yr

"""avg_diff_grid = xr.Dataset(
    data_vars=dict(
    NO=(["time","lat", "lon"], avg_grid_NO),
    NO2=(["time","lat", "lon"], avg_grid_NO2),
    SO2=(["time","lat", "lon"], avg_grid_SO2),
    ),
    coords=midx_coords,
    )

# save to Dataset to path
path='~/us_ego/annual_emissions/'
name = (f'inventory_power_plants_{case_name}_average.nc')
avg_diff_grid.to_netcdf(path+name)"""
