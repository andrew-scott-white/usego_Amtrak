#!/bin/bash

sbatch runopt.sh 1 500
sbatch runopt.sh 501 1000
sbatch runopt.sh 1001 1500
sbatch runopt.sh 1501 2000
sbatch runopt.sh 2001 2500
sbatch runopt.sh 2501 3000
sbatch runopt.sh 3001 3500
sbatch runopt.sh 3501 4000
sbatch runopt.sh 4001 4500

sbatch runopt.sh 4501 5000
sbatch runopt.sh 5001 5500
sbatch runopt.sh 5501 6000
sbatch runopt.sh 6001 6500
sbatch runopt.sh 6501 7000
sbatch runopt.sh 7001 7500
sbatch runopt.sh 7501 8000
sbatch runopt.sh 8001 8500
sbatch runopt.sh 8501 8759

