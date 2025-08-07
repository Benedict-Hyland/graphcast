#!/bin/bash --login

#SBATCH --ntasks=1
#SBATCH --mem=10g
#SBATCH --account=nems
#SBATCH --partition=u1-service
#SBATCH --time=10:00
#SBATCH --job-name=solo-prepdata
#SBATCH --output=slurm/solo_prepdata.out
#SBATCH --error=slurm/solo_prepdata.err

# load necessary modules
module use /contrib/spack-stack/spack-stack-1.9.1/envs/ue-oneapi-2024.2.1/install/modulefiles/Core/
module load stack-oneapi
module load wgrib2
module list

# Activate Conda environment
source /scratch3/NCEPDEV/nems/Linlin.Cui/miniforge3/etc/profile.d/conda.sh
conda activate graphcast

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
curr_datetime=$( date -d "$datetime 6 hour ago" "+%Y%m%d%H" )
prev_datetime=$( date -d "$datetime 12 hour ago" "+%Y%m%d%H" )

echo "Current state: $curr_datetime"
echo "6 hours earlier state: $prev_datetime"

forecast_length=64
echo "forecast length: $forecast_length"

num_pressure_levels=13
echo "number of pressure levels: $num_pressure_levels"

start_time=$(date +%s)
echo "start runing gdas utility to generate graphcast inputs for: $curr_datetime"
# Run the Python script gdas.py with the calculated times
python gdas_utility.py "$prev_datetime" "$curr_datetime" -l "$num_pressure_levels"

end_time=$(date +%s)  # Record the end time in seconds since the epoch

# Calculate and print the execution time
execution_time=$((end_time - start_time))
echo "Execution time for gdas_utility.py $execution_time seconds"

# Get TC tracker data
module load awscli-v2/2.15.53
aws s3 --profile gcgfs sync s3://noaa-nws-graphcastgfs-pds/hurricanes/syndat tracker/syndat
