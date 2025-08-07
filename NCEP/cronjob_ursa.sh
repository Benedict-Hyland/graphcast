#!/bin/bash --login

echo "Job 1 is running"
sh /scratch3/NAGAPE/gpu-ai4wp/Linlin.Cui/eagle_solo/gc_prepdata_ursa.sh
sleep 60  # Simulating some work
echo "Job 1 completed"

echo "Job 2 is running"
job2_id=$(sbatch /scratch3/NAGAPE/gpu-ai4wp/Linlin.Cui/eagle_solo/gc_runfcst_ursa_gpu.sh | awk '{print $4}')

# Wait for job 2 to complete
while squeue -j $job2_id &>/dev/null; do
    sleep 5  # Adjust the polling interval as needed
done
sleep 5  # Simulating some work
echo "Job 2 completed"

echo "Job 3: running TC tracker"
job3_id=$(sbatch /scratch3/NAGAPE/gpu-ai4wp/Linlin.Cui/eagle_solo/jAIGFS_cyclone_track_00.ecf_ursa | awk 'print $4')

# Wait for job 3 to complete
while squeue -j $job3_id &>/dev/null; do
    sleep 5  # Adjust the polling interval as needed
done
sleep 5  # Simulating some work
echo "Job 3 completed"

echo "Job 4 is running"
sh /scratch3/NAGAPE/gpu-ai4wp/Linlin.Cui/eagle_solo/gc_datadissm_ursa.sh
sleep 5  # Simulating some work
echo "Job 4 completed"
