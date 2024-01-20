#!/bin/bash

# load necessary modules
module use /scratch1/NCEPDEV/nems/role.epic/spack-stack/spack-stack-1.5.1/envs/unified-env/install/modulefiles/Core
module load stack-intel
module load awscli
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

# Activate Conda environment
source /scratch1/NCEPDEV/nems/AIML/miniconda3/etc/profile.d/conda.sh
conda activate mlwp


start_time=$(date +%s)
echo "start uploading graphcast forecast to s3 bucket for: $curr_datetime"
# Run another Python script
python3 upload_to_s3bucket.py -i source-gdas_date-"$curr_datetime"_res-0.25_levels-13_steps-2.nc -o forecast_date-"$curr_datetime"_res-0.25_levels-13_steps-"$forecast_length".nc

end_time=$(date +%s)  # Record the end time in seconds since the epoch

# Calculate and print the execution time
execution_time=$((end_time - start_time))
echo "Execution time for uploading to s3 bucket: $execution_time seconds"
