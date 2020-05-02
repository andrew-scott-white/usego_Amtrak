# us-ego
Warning: the input csv files are not included here. They are stored in our current Dropbox folder on Guillaume's server (https://www.dropbox.com/sh/edxgc11umqw4y3b/AABlT1BPtHBfTTVNdsR5dA5da?dl=0)

1. Run modify_cost_inputs, which loads Alan's input data from ./good_model_inputs/ and adjusts the costs and capacities. This gives you a new generation and transmission file.

2. Run check_feasibility.ipynb on the new generation and transmission files if you want to check whether or not there is a time where demand exceeds generation capacity+import capacity.

3. Launch your terminal and create a directory called 'outputs'

4. Run ./launch.sh, which will run the optimization (sliced into 8 different runs)
    - To edit the limit for time and CPUs for the run, edit runopt.sh. This is what launches runopt.jl, your optimization script, and is launched by "sbatch runopt.sh t1 t2" where t1 is the first hour of the simulation and t2 is the last one
    - To edit the time slices for the run, edit launch.sh
    - Your run will take anywhere from 1-2 days, after which you can look at the output
    - Edit the input files for runopt.jl (based on the output from 1) 

6. Run combined.ipynb to merge and format the outputs

7. Validate your output against eGrid generation and emissions with postproc_opt.ipynb, which takes your combined outputs

8. If you want to compare your data to NEI 2016 data, use NEI_validation.ipynb for a monthly validation (data from ftp://newftp.epa.gov/DMDnLoad/emissions/daily/quarterly/2016/)

9. To get the data in a format readable by GEOS-Chem, with access to a slurm run sbatch opt_output_to_inventory.py

Additional notes: test_run_opt.ipynb is the draft Julia optmization script
