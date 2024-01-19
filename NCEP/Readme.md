# GraphCast model with NCEP GDAS Products as ICs

This repository provides scripts to run real-time GraphCast using GDAS products as inputs. There are multiple scripts in the repository including:
- `gdas_utility.py`: a Python script designed to download Global Data Assimilation System (GDAS) data from the National Centers for Environmental Prediction (NCEP) from NOAA S3 bucket (or NOMADS), and prepare the data in a format suitable for feeding into the GraphCast weather prediction system.
- `run_graphcast.py`: a Python script that calls GraphCast and takes GDAS products as input and produces six-hourly forecasts with an arbitrary forecast length (e.g., 40 --> 10-days).
- `graphcast_job_[machine_name].sh`: a Bash script that automates running GraphCast in real-time over the AWS cloud machine (should be submitted through CronJob).

## Table of Contents
- [Overview](#overview)
- [Prerequisites and Installation](#prerequisites-and-installation)
- [Usage](#usage)
  - [GDAS Utility](#gdas-utility)
  - [Run GraphCast](#run-graphcast)
- [Options](#options)
- [Output](#output)
- [Contact](#contact)

## Overview

The National Centers for Environmental Prediction (NCEP) provides GDAS data that can be used for weather prediction and analysis. This repository simplifies the process of downloading GDAS data, extracting relevant variables, and converting it into a format compatible with the GraphCast weather prediction system. In addition, it automates running GraphCast with GDAS inputs on the AWS cloud.

## Prerequisites and Installation

To install the package, run:

```bash
conda create --name mlwp python=3.10
conda activate mlwp
pip install dm-tree
pip install boto3
pip install xarray
pip install netcdf4
conda install --channel conda-forge cartopy
pip install --upgrade https://github.com/deepmind/graphcast/archive/master.zip
```

This will install the packages and most of their dependencies.


Additionally, the utility uses the `wgrib2` library for extracting specific variables from the GDAS data. You can download and install `wgrib2` from [here](http://www.cpc.ncep.noaa.gov/products/wesley/wgrib2/). Make sure it is included in your system's PATH.

## Usage

To use the utility, follow these steps:

1. Clone this repository:
   
   `git clone https://github.com/NOAA-EMC/graphcast.git`
   
   `cd graphcast/NCEP`

3. Submit the `graphcast_job.sh` to run for the most recent forecasts or run the above scripts with the desired start and end datetime (in "YYYYMMDDHH" format).

   `sbatch graphcast_job.sh`


3. Optional: Specify additional options (see [Options](#options)).

## Options

The `gdas_utility.py` supports the following optional command-line arguments:

- `-o, --output-dir <output_directory>`: Specify the directory to save the processed data (default: "./output/").
- `-d, --download-dir <download_directory>`: Specify the directory to save downloaded data (default: "./data/").
- `-k, --keep-data <yes/no>`: Specify whether to keep downloaded data after processing (default: "yes").
- `-h, --help`: Display help information.

Example usage with options:

   `python gdas_utility.py 2023060600 2023060712 -o /path/to/output -d /path/to/download -k no`


## Output

The processed GDAS data as well as GraphCast forecasts will be saved in NetCDF format in the output and forecast directories specified. The files will be named based on the date.



For questions or issues, please contact [Sadegh.Tabas@noaa.gov](mailto:Sadegh.Tabas@noaa.gov).
