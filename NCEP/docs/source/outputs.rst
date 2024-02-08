######################
Product
######################

The GraphCastGFS model runs 4 times a day at 00Z, 06Z, 12Z, and 18Z cycles, which both applies on 13 levels and 37-levels. 
The near real-time forecast outputs along with inputs are available on `AWS <https://noaa-nws-graphcastgfs-pds.s3.amazonaws.com/index.html>`_. For each cycle, the dataset contains input files to feed into GraphCast found in the directory ./graphcastgfs.yyyymmdd/hh/input 
and 10-day forecast results for the current cycle found iin the directory ./graphcastgfs.yyyymmdd/hh/forecasts_13_levels and 
./graphcastgfs.yyyymmdd/hh/forecasts_37_levels, respectively. 
