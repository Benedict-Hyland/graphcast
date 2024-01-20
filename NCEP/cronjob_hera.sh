#!/bin/bash --login
sh /scratch1/NCEPDEV/nems/AIML/graphcast/NCEP/gc_job1_hera.sh
echo "Job 1 is running"
sleep 60  # Simulating some work
echo "Job 1 completed"

sbatch /scratch1/NCEPDEV/nems/AIML/graphcast/NCEP/gc_job2_hera.sh
job2_id=$(sbatch /scratch1/NCEPDEV/nems/AIML/graphcast/NCEP/gc_job2_hera.sh | awk '{print $4}')
echo "Job 2 is running"
sleep 5  # Simulating some work
echo "Job 2 completed"

# Wait for job 2 to complete
wait $job2_id

sh /scratch1/NCEPDEV/nems/AIML/graphcast/NCEP/gc_job3_hera.sh
echo "Job 3 is running"
sleep 5  # Simulating some work
echo "Job 3 completed"
