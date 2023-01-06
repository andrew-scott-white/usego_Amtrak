#!/bin/bash

#SBATCH --time=2-00:00
#SBATCH --cpus-per-task=4
#SBATCH -N 1
#SBATCH -p fdr

. ~/.bash_profile

julia runopt.jl $1 $2
