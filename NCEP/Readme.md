# NCEP GDAS Data Processing Utility to Provide GraphCast Inputs

The NCEP GDAS Data Processing Utility is a Python script designed to download Global Data Assimilation System (GDAS) data from the National Centers for Environmental Prediction (NCEP) S3 bucket, and prepare the data in a format suitable for input into the GraphCast weather prediction system.

## Table of Contents
- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Usage](#usage)
- [Options](#options)
- [Output](#output)

## Overview

The National Centers for Environmental Prediction (NCEP) provides GDAS data that can be used for weather prediction and analysis. This utility simplifies the process of downloading GDAS data, extracting relevant variables, and converting it into a format compatible with the GraphCast weather prediction system.

## Prerequisites

Before using this utility, ensure you have the following prerequisites installed:
- Python 3.x
- Required Python packages (install using `pip install -r requirements.txt`):
  - `xarray`
  - `boto3`

Additionally, the utility uses the `wgrib2` library for extracting specific variables from the GDAS data. You can download and install `wgrib2` from [here](http://www.cpc.ncep.noaa.gov/products/wesley/wgrib2/). Make sure it is included in your system's PATH.

## Usage

To use the utility, follow these steps:

1. Clone this repository:
   
   `git clone https://github.com/SadeghTabas-NOAA/graphcast.git`
   
   `cd graphcast/NCEP`

3. Run the script with the desired start and end datetime (in "YYYYMMDDHH" format) as arguments:

   `python utility.py 2023060600 2023060712`


3. Optional: Specify additional options (see [Options](#options)).

## Options

The utility supports the following optional command-line arguments:

- `-o, --output-dir <output_directory>`: Specify the directory to save the processed data (default: "./output/").
- `-d, --download-dir <download_directory>`: Specify the directory to save downloaded data (default: "./data/").
- `-k, --keep-data <yes/no>`: Specify whether to keep downloaded data after processing (default: "yes").
- `-h, --help`: Display help information.

Example usage with options:

   `python utility.py 2023060600 2023060712 -o /path/to/output -d /path/to/download -k no`


## Output

The processed GDAS data will be saved in NetCDF format in the output directory specified. The files will be named based on the date and number of time steps in the data.



For questions or issues, please contact [Sadegh.Tabas@noaa.gov](mailto:Sadegh.Tabas@noaa.gov).
