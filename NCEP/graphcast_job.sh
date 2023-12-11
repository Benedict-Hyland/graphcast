#!/bin/bash
#SBATCH --nodes=1
#SBATCH --cpus-per-task=$(nproc)  # Use all available CPU cores
#SBATCH --time=1:30:00  # Adjust this to your estimated run time
#SBATCH --job-name=graphcast
#SBATCH --output=graphcast_job_output.txt
#SBATCH --error=graphcast_job_error.txt


# load necessary modules
module use /contrib/spack-stack/envs/ufswm/install/modulefiles/Core/
module load stack-intel
module load wgrib2
module list
