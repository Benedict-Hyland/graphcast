#!/bin/bash
#SBATCH --nodes=1
#SBATCH --account=nems
#SBATCH --cpus-per-task=40 
#SBATCH --time=4:00:00 
#SBATCH --job-name=graphcast
#SBATCH --output=gc_output.txt
#SBATCH --error=gc_error.txt
#SBATCH --partition=hera


# load necessary modules
module use /scratch1/NCEPDEV/nems/role.epic/spack-stack/spack-stack-1.6.0/envs/unified-env/install/modulefiles/Core
module load stack-intel
module load wgrib2
module load awscli-v2
module list


# Get the UTC hour and calculate the time in the format yyyymmddhh
current_hour=$(date -u +%H)
current_hour=$((10#$current_hour))

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
curr_datetime=$( date -d "$datetime 12 hour ago" "+%Y%m%d%H" )
prev_datetime=$( date -d "$datetime 18 hour ago" "+%Y%m%d%H" )

echo "Current state: $curr_datetime"
echo "6 hours earlier state: $prev_datetime"

forecast_length=40
echo "forecast length: $forecast_length"

num_pressure_levels=13
echo "number of pressure levels: $num_pressure_levels"

# Activate Conda environment
source /scratch1/NCEPDEV/nems/AIML/miniconda3/etc/profile.d/conda.sh
conda activate mlwp

cd /scratch1/NCEPDEV/nems/AIML/graphcast/NCEP/

start_time=$(date +%s)
echo "start runing graphcast to get real time 10-days forecasts for: $curr_datetime"
# Run another Python script
python3 run_graphcast.py -i source-gdas_date-"$curr_datetime"_res-0.25_levels-"$num_pressure_levels"_steps-2.nc -w /scratch1/NCEPDEV/nems/AIML/gc_weights -l "$forecast_length" -p "$num_pressure_levels"

end_time=$(date +%s)  # Record the end time in seconds since the epoch

# Calculate and print the execution time
execution_time=$((end_time - start_time))
echo "Execution time for graphcast: $execution_time seconds"
