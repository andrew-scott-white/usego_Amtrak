#!/bin/bash

#SBATCH --time=2-00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=60G
#SBATCH -N 1
. ~/.bash_profile

julia runopt.jl $1 $2

