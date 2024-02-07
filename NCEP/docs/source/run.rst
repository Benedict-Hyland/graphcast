######################
Run GraphCastGFS
######################
In order to run GraphCast in inference mode you will also need to have the model weights, normalization statistics, which are avaiable on `Google Cloud Bucket <https://console.cloud.google.com/storage/browser/dm_graphcast;tab=objects?prefix=&forceOnObjectsSortingFiltering=false&pageState=(%22StorageObjectListTable%22:(%22f%22:%22%255B%255D%22))>`_ 
Once you have input netCDF file, model weights, and statistics data, you can run the GraphCast model with a leading time (e.g., leading time 10 days will result in forecast_length of 40) using::
    python run_graphcast.py --input /input/filename/with/path --output /path/to/output --weights /path/to/weights --length forecast_length
 .. code-block:: rst

Arguments:
  Required:
    -i or --input: /input/filename/with/path 
    -o or --output: /path/to/output
    -w or --weights: /path/to/weights/and/stats
    -l or --length: integer, the number of forecast time steps (6-hourly)
  Optional:
    -u or --upload: yes or no, upload input and output files to NOAA s3 bucket (default: no)
    -k or --keep: yes or no, whether to keep input and output files after uploading
