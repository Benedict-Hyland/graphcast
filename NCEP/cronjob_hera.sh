#!/bin/bash --login
sh /scratch1/NCEPDEV/nems/AIML/graphcast/NCEP/gc_job1_hera.sh
sbatch /scratch1/NCEPDEV/nems/AIML/graphcast/NCEP/gc_job2_hera.sh
sh /scratch1/NCEPDEV/nems/AIML/graphcast/NCEP/gc_job3_hera.sh
