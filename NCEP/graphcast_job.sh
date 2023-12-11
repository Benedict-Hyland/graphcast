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

# Get the UTC hour and calculate the time in the format yyyymmddhh
current_hour=$(date -u +%H)
if (( $current_hour >= 0 && $current_hour < 6 )); then
    datetime=$(date -u -d 'today 00:00')
elif (( $current_hour >= 6 && $current_hour < 12 )); then
    datetime=$(date -u -d 'today 06:00')
elif (( $current_hour >= 12 && $current_hour < 18 )); then
    datetime=$(date -u -d 'today 12:00')
else
    datetime=$(date -u -d 'today 18:00')
fi

# Calculate time 6 hours before
#curr_datetime=$(date -u -d "$time" +'%Y%m%d%H')
curr_datetime=$( date -d "$time 6 hour ago" "+%Y%m%d%H" )
prev_datetime=$( date -d "$time 12 hour ago" "+%Y%m%d%H" )

echo "Current state: $curr_datetime"
echo "6 hours earlier state: $prev_datetime"

# Set Miniconda path
export PATH="/contrib/Sadegh.Tabas/miniconda3/bin:$PATH"

# Activate Conda environment
source /contrib/Sadegh.Tabas/miniconda3/etc/profile.d/conda.sh
conda activate graphcast
