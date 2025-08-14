#!/bin/bash --login

#SBATCH --ntasks=1
#SBATCH --account=nems
#SBATCH --partition=u1-service
#SBATCH --time=10:00
#SBATCH --job-name=eagle-solo
#SBATCH --output=slurm/gcgfs_datadissm.out
#SBATCH --error=slurm/gcgfs_datadissm.err

# load modules
module use /contrib/spack-stack/spack-stack-1.9.1/envs/ue-oneapi-2024.2.1/install/modulefiles/Core/
module load stack-oneapi
module load awscli-v2/2.15.53
module list

curr_datetime=$1
num_pressure_levels=13
COMROOT=/scratch3/NCEPDEV/stmp/Linlin.Cui/ptmp

echo "Current state: $curr_datetime"

start_time=$(date +%s)
echo "Uploading member $gefs_member for: $curr_datetime"

## Extract the date and hour parts
ymd=${curr_datetime:0:8}
hour=${curr_datetime:8:2}

## upload forecast outputs
#aws s3 --profile gcgfs sync $curr_datetime/forecasts_13_levels/ s3://noaa-nws-graphcastgfs-pds/graphcastgfs."$ymd"/"$hour"/forecasts_13_levels/
#rm -r $curr_datetime/forecasts_13_levels
#
## upload input file
#aws s3 --profile gcgfs cp $curr_datetime/source-gdas_date-"$curr_datetime"_res-0.25_levels-"$num_pressure_levels"_steps-2.nc s3://noaa-nws-graphcastgfs-pds/graphcastgfs."$ymd"/"$hour"/input/
#rm $curr_datetime/source-gdas_date-"$curr_datetime"_res-0.25_levels-"$num_pressure_levels"_steps-2.nc

#upload tc tracker file
modelname="mgfs"
#modelname=$(echo "$modelname" | tr '[:lower:]' '[:upper:]')
tctracker=$COMROOT/eagle_solo/aigfs.$ymd/$hour/products/atmos/cyclone/tracks/${modelname}p.t${hour}z.cyclone.trackatcfunix

if [ -s "$tctracker" ]; then
    echo "Uploading the tracker file to s3 bucket:"
    aws s3 --profile gcgfs cp ${tctracker} s3://noaa-nws-graphcastgfs-pds/graphcastgfs."$ymd"/"$hour"/forecasts_13_levels/${modelname}p.t${hour}z.cyclone.trackatcfunix
else
    echo "tracker file for ${ymd}${hour}is empty!"
fi

# copy tracker file to noscrub folder
cp -r $COMROOT/eagle_solo/aigfs.$ymd  /scratch3/NCEPDEV/nems/MGFS/track_forecast/eagle_solo

end_time=$(date +%s)  # Record the end time in seconds since the epoch
# Calculate and print the execution time
execution_time=$((end_time - start_time))
echo "Execution time for uploading: $execution_time seconds"
