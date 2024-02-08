######################
Product
######################

The GraphCastGFS model runs 4 times a day at 00Z, 06Z, 12Z, and 18Z cycles, which applies to both 13 levels and 37-levels. The 13 pressure levels 
are 50, 100, 150, 200, 250, 300, 400, 500, 600, 700, 850, 925, and 1000 hPa. The 37 pressure levels include 1, 2, 3, 5, 7, 10, 20, 30, 50, 70, 100, 
125, 150, 175, 200, 225, 250, 300, 350, 400, 450, 500, 550, 600, 650, 700, 750, 800, 825, 850, 875, 900, 925, 950, 975, and 1000 hPa. 
The near real-time forecast outputs along with inputs are available on `AWS <https://noaa-nws-graphcastgfs-pds.s3.amazonaws.com/index.html>`_. 
For each cycle, the dataset contains input files to feed into GraphCast found in the directory graphcastgfs.yyyymmdd/hh/input 
and 10-day forecast results for the current cycle found in the directory graphcastgfs.yyyymmdd/hh/forecasts_13_levels and 
graphcastgfs.yyyymmdd/hh/forecasts_37_levels, respectively. Major surface and atmospheric fields are available, including 10-m U and V components of wind, 
2-m temperature, mean sea-level pressure, total precipitation for surface variables, and temperature, U and V component of wind, geopotential, 
specific humidity for atmospheric variables.
