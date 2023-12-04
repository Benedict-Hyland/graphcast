'''
Description
@uthor: Sadegh Sadeghi Tabas (sadegh.tabas@noaa.gov)
Revision history:
    -20231010: Sadegh Tabas, initial code
    -20231204: Sadegh Tabas, updating units and resolving memory issue
'''
import os
import sys
import boto3
import xarray as xr
import subprocess
import numpy as np
from datetime import datetime, timedelta
from botocore.config import Config
from botocore import UNSIGNED
import argparse

class GFSDataProcessor:
    def __init__(self, start_datetime, end_datetime, output_directory=None, download_directory=None, keep_downloaded_data=True):
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime
        self.output_directory = output_directory
        self.download_directory = download_directory
        self.keep_downloaded_data = keep_downloaded_data

        # Initialize the S3 client
        self.s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))

        # Specify the S3 bucket name and root directory
        self.bucket_name = 'noaa-gfs-bdp-pds'
        self.root_directory = 'gdas'

        # Specify the local directory where you want to save the files
        if self.download_directory is None:
            self.local_base_directory = os.path.join(os.getcwd(), 'noaa-gfs-bdp-pds-data')  # Use current directory if not specified
        else:
            self.local_base_directory = os.path.join(self.download_directory, 'noaa-gfs-bdp-pds-data')

        # List of file formats to download
        self.file_formats = ['0p25.f000', '0p25.f006'] # , '0p25.f001'

    def download_data(self):
        # Calculate the number of 6-hour intervals
        delta = (self.end_datetime - self.start_datetime)
        total_intervals = int(delta.total_seconds() / 3600 / 6)  # 6 hours per interval

        # Loop through the 6-hour intervals
        current_datetime = self.start_datetime
        while current_datetime <= self.end_datetime:
            date_str = current_datetime.strftime("%Y%m%d")
            time_str = current_datetime.strftime("%H")

            # Construct the S3 prefix for the directory
            s3_prefix = f"{self.root_directory}.{date_str}/{time_str}/"

            # List objects in the S3 directory
            s3_objects = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=s3_prefix)

            # Filter objects based on the desired formats
            for obj in s3_objects.get('Contents', []):
                obj_key = obj['Key']
                for file_format in self.file_formats:
                    if obj_key.endswith(f'.{file_format}'):
                        # Define the local directory path where the file will be saved
                        local_directory = os.path.join(self.local_base_directory, date_str, time_str)

                        # Create the local directory if it doesn't exist
                        os.makedirs(local_directory, exist_ok=True)

                        # Define the local file path
                        local_file_path = os.path.join(local_directory, os.path.basename(obj_key))

                        # Download the file from S3 to the local path
                        self.s3.download_file(self.bucket_name, obj_key, local_file_path)
                        print(f"Downloaded {obj_key} to {local_file_path}")

            # Move to the next 6-hour interval
            current_datetime += timedelta(hours=6)

        print("Download completed.")

    def process_data(self):
        # Define the directory where your GRIB2 files are located
        data_directory = self.local_base_directory

        # Create a dictionary to specify the variables, levels, and whether to extract only the first time step (if needed)
        variables_to_extract = {
            '.f000': {
                ':HGT:': {
                    'levels': [':surface:'],
                    'first_time_step_only': True,  # Extract only the first time step
                },
                ':TMP:': {
                    'levels': [':2 m above ground:'],
                },
                ':PRES:': {
                    'levels': [':surface:'],
                },
                ':VGRD:': {
                    'levels': [':10 m above ground:'],
                },
                ':UGRD:': {
                    'levels': [':10 m above ground:'],
                },
                ':SPFH|DZDT|VGRD|UGRD|HGT|TMP:': {
                    'levels': [':(50|100|150|200|250|300|400|500|600|700|850|925|1000) mb:'],
                },
            },
            #'.f001': {
            #    ':USWRF:': {
            #        'levels': [':top of atmosphere:'],
            #    },
            #},
            '.f006': {
                ':LAND:': {
                    'levels': [':surface:'],
                    'first_time_step_only': True,  # Extract only the first time step
                },
                '^(597):': {  # APCP
                    'levels': [':surface:'],
                },
            }
        }

        # Create an empty list to store the extracted datasets
        extracted_datasets = []
        print("Start extracting variables and associated levels from grib2 files:")
        # Loop through each folder (e.g., gdas.yyyymmdd)
        date_folders = sorted(next(os.walk(data_directory))[1])
        for date_folder in date_folders:
            date_folder_path = os.path.join(data_directory, date_folder)

            # Loop through each hour (e.g., '00', '06', '12', '18')
            for hour in ['00', '06', '12', '18']:
                subfolder_path = os.path.join(date_folder_path, hour)

                # Check if the subfolder exists before processing
                if os.path.exists(subfolder_path):
                    # Loop through each GRIB2 file (.f000, .f001, .f006)
                    for file_extension, variable_data in variables_to_extract.items():
                        for variable, data in variable_data.items():
                            levels = data['levels']
                            first_time_step_only = data.get('first_time_step_only', False)  # Default to False if not specified

                            grib2_file = os.path.join(subfolder_path, f'gdas.t{hour}z.pgrb2.0p25{file_extension}')

                            # Extract the specified variables with levels from the GRIB2 file
                            for level in levels:
                                output_file = f'{variable}_{level}_{date_folder}_{hour}{file_extension}.nc'

                                # Use wgrib2 to extract the variable with level
                                wgrib2_command = ['wgrib2', '-nc_nlev', '13', grib2_file, '-match', f'{variable}', '-match', f'{level}', '-netcdf', output_file]
                                subprocess.run(wgrib2_command, check=True)

                                # Open the extracted netcdf file as an xarray dataset
                                ds = xr.open_dataset(output_file)

                                if variable == '^(597):':
                                    ds['time'] = ds['time'] - np.timedelta64(6, 'h')
                                elif variable == ':USWRF:':
                                    ds['time'] = ds['time'] - np.timedelta64(1, 'h')

                                # If specified, extract only the first time step
                                if variable not in [':LAND:', ':HGT:']:
                                    # Append the dataset to the list
                                    extracted_datasets.append(output_file)
                                else:
                                    if first_time_step_only:
                                        # Append the dataset to the list
                                        ds = ds.isel(time=0)
                                        extracted_datasets.append(output_file)
                                        variables_to_extract[file_extension][variable]['first_time_step_only'] = False
                                ds.to_netcdf(output_file)
                                # Optionally, remove the intermediate GRIB2 file
                                # os.remove(output_file)
        print("Merging grib2 files:")
        ds = xr.open_dataset(extracted_datasets[0])
        for file in extracted_datasets[1:]:
            currDS = xr.open_dataset(file)
            ds = xr.merge([ds, currDS])
            
            os.remove(file)
        print("Merging process completed.")
        
        print("Processing, Renaming and Reshaping the data:")
        # Drop the 'level' dimension
        ds = ds.drop_dims('level')

        # Rename variables and dimensions
        ds = ds.rename({
            'latitude': 'lat',
            'longitude': 'lon',
            'plevel': 'level',
            'HGT_surface': 'geopotential_at_surface',
            'LAND_surface': 'land_sea_mask',
            'PRES_surface': 'mean_sea_level_pressure',
            'TMP_2maboveground': '2m_temperature',
            'UGRD_10maboveground': '10m_u_component_of_wind',
            'VGRD_10maboveground': '10m_v_component_of_wind',
            'APCP_surface': 'total_precipitation_6hr',
            'USWRF_topofatmosphere': 'toa_incident_solar_radiation',
            'HGT': 'geopotential',
            'TMP': 'temperature',
            'SPFH': 'specific_humidity',
            'DZDT': 'vertical_velocity',
            'UGRD': 'u_component_of_wind',
            'VGRD': 'v_component_of_wind'
        })

        # Assign 'datetime' as coordinates
        ds = ds.assign_coords(datetime=ds.time)

        # Convert data types
        ds['lat'] = ds['lat'].astype('float32')
        ds['lon'] = ds['lon'].astype('float32')
        ds['level'] = ds['level'].astype('int32')

        # Adjust time values relative to the first time step
        ds['time'] = ds['time'] - ds.time[0]

        # Expand dimensions
        ds = ds.expand_dims(dim='batch')
        ds['datetime'] = ds['datetime'].expand_dims(dim='batch')

        # Squeeze dimensions
        ds['geopotential_at_surface'] = ds['geopotential_at_surface'].squeeze('batch')
        ds['land_sea_mask'] = ds['land_sea_mask'].squeeze('batch')

        # Update geopotential unit to m2/s2 by multiplying 9.80665
        ds['geopotential_at_surface'] = ds['geopotential_at_surface'] * 9.80665
        ds['geopotential'] = ds['geopotential'] * 9.80665

        # Update total_precipitation_6hr unit to (m) from (kg/m^2) by dividing it by 1000kg/m³
        ds['total_precipitation_6hr'] = ds['total_precipitation_6hr'] / 1000

        # Update USWRF (w/m^2), to be used as ERA5 toa_incident_solar_radiation (J/m^2); ( x 3600s)
        ds['toa_incident_solar_radiation'] = ds['toa_incident_solar_radiation'] * 3600
        # Define the output NetCDF file
        date = date_folders[0]
        steps = str(len(ds['time']))

        if self.output_directory is None:
            self.output_directory = os.getcwd()  # Use current directory if not specified
        output_netcdf = os.path.join(self.output_directory, f"source-gdas_date-{date}_res-0.25_levels-13_steps-{steps}.nc")

        # Save the merged dataset as a NetCDF file
        ds.to_netcdf(output_netcdf)
        print(f"Saved output to {output_netcdf}")

        # Optionally, remove downloaded data
        if not self.keep_downloaded_data:
            self.remove_downloaded_data()

        print("Processing completed.")

    def remove_downloaded_data(self):
        # Remove downloaded data from the specified directory
        if self.download_directory is not None:
            print("Removing downloaded data...")
            try:
                os.system(f"rm -rf {self.download_directory}")
                print("Downloaded data removed.")
            except Exception as e:
                print(f"Error removing downloaded data: {str(e)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download and process GFS data")
    parser.add_argument("start_datetime", help="Start datetime in the format 'YYYYMMDDHH'")
    parser.add_argument("end_datetime", help="End datetime in the format 'YYYYMMDDHH'")
    parser.add_argument("-o", "--output", help="Output directory for processed data")
    parser.add_argument("-d", "--download", help="Download directory for raw data")
    parser.add_argument("-k", "--keep", help="Keep downloaded data (yes or no)", default="yes")

    args = parser.parse_args()

    start_datetime = datetime.strptime(args.start_datetime, "%Y%m%d%H")
    end_datetime = datetime.strptime(args.end_datetime, "%Y%m%d%H")
    output_directory = args.output
    download_directory = args.download
    keep_downloaded_data = args.keep.lower() == "yes"

    data_processor = GFSDataProcessor(start_datetime, end_datetime, output_directory, download_directory, keep_downloaded_data)
    data_processor.download_data()
    data_processor.process_data()
