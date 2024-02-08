#############################################
Preparing inputs from GDAS product
#############################################

GraphCast takes two states of the weather (current and 6-hr earlier states) as the initial conditions. We will create a netCDF file containing these two states from GDAS 0.25 degree reanalysis data. This can be performed using the script NCEP/gdas_utility.py. The script downloads the GDAS data from either NOAA s3 bucket or NOAA NOMADS server, which are in GRIB2 format. Then it extracts required variables from GRIB2 files and saves data as netCDF files. Run the script using::

    python gdas_utility.py startdate enddate --level 13 --source s3 --output /path/to/output --download /path/to/download --method wgrib2 --keep no

**Arguments**

Requried:

    startdate and endate: string, yyyymmddhh

Optional:

    *-l* or *--level*: 13 or 37, the number of pressure levels (default: 13) 

    *-s* or *--source*: s3 or nomads, the sourece to download gdas data (default: s3) 

    *-m* or *--method*: wgrib2 or pygrib, the method to extract required variables and create netCDF file (default: wgrib2) 

    *-o* or *--output*: /path/to/output, where to save forecast outputs  (default: current directory) 

    *-d* or *--download*: /path/to/download, where to save downloaded grib2 files (default: current directory) 

    *-k* or *--keep*: yes or no, whether to keep downloaded data after processed (default: no) 
