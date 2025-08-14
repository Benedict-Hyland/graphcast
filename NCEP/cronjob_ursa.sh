#!/bin/bash --login

cd /scratch3/NCEPDEV/nems/Linlin.Cui/git/LinlinCui-NOAA/graphcast/NCEP
#directory where cronjob_ursa.sh
SCRIPT_DIR="$(cd "$(dirname "${BASH_DOURCE[0]}")" && pwd)"
echo $SCRIPT_DIR

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

echo "Job 1 is running"
sh $SCRIPT_DIR/gc_prepdata_ursa.sh $curr_datetime $prev_datetime
sleep 60  # Simulating some work
echo "Job 1 completed"

echo "Job 2 is running"
job2_id=$(sbatch "$SCRIPT_DIR"/gc_runfcst_ursa_gpu.sh $curr_datetime| awk '{print $4}')

# Wait for job 2 to complete
while squeue -j $job2_id &>/dev/null; do
    sleep 5  # Adjust the polling interval as needed
done
sleep 5  # Simulating some work
echo "Job 2 completed"

echo "Job 3: running TC tracker"
job3_id=$(sbatch "$SCRIPT_DIR"/jAIGFS_cyclone_track_00.ecf_ursa $curr_datetime "" | awk '{print $4}')

## Wait for job 3 to complete
#while squeue -j $job3_id &>/dev/null; do
#    sleep 5  # Adjust the polling interval as needed
#done
#sleep 5  # Simulating some work
#echo "Job 3 completed"
#
#echo "Job 4 is running"
#sh $SCRIPT_DIR/gc_datadissm_ursa.sh $curr_datetime
#sleep 5  # Simulating some work
#echo "Job 4 completed"
