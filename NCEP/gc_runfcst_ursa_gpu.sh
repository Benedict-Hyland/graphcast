#!/bin/bash --login

#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --account=gpu-ai4wp
#SBATCH --time=30:00
#SBATCH --job-name=solo_fcst
#SBATCH --output=slurm/solo_fcst.out
#SBATCH --error=slurm/solo_fcst.err
#SBATCH --partition=u1-h100
#SBATCH --qos=gpuwf
#SBATCH --gres=gpu:h100:1
#SBATCH --exclusive

# load necessary modules
module use /contrib/spack-stack/spack-stack-1.9.1/envs/ue-oneapi-2024.2.1/install/modulefiles/Core/
module load stack-oneapi
module load wgrib2

source /scratch3/NCEPDEV/nems/Linlin.Cui/miniforge3/etc/profile.d/conda.sh
conda activate graphcast

curr_datetime=$1
echo "Current state: $curr_datetime"
#echo "6 hours earlier state: $prev_datetime"

echo "Current state: $curr_datetime"
forecast_length=64
echo "forecast length: $forecast_length"

num_pressure_levels=13
echo "number of pressure levels: $num_pressure_levels"

model_weights=/scratch3/NCEPDEV/nems/MGFS/graphcast/gc_weights
echo "Model weights and stats are at: $model_weights"

start_time=$(date +%s)
echo "start runing graphcast to get real time 10-days forecasts for: $curr_datetime"

numactl --interleave=all python run_graphcast.py -i source-gdas_date-"$curr_datetime"_res-0.25_levels-"$num_pressure_levels"_steps-2.nc -w $model_weight -l "$forecast_length" -p "$num_pressure_levels" -m grib2io -u no -k yes

end_time=$(date +%s)  # Record the end time in seconds since the epoch

# Calculate and print the execution time
execution_time=$((end_time - start_time))
echo "Execution time for graphcast: $execution_time seconds"
