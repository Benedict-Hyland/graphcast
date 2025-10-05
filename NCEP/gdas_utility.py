'''
Description
@uthor: Sadegh Sadeghi Tabas (sadegh.tabas@noaa.gov)
Revision history:
    -20231010: Sadegh Tabas, initial code
    -20231204: Sadegh Tabas, calculating toa incident solar radiation, parallelizing, updating units, and resolving memory issues
    -20240112: Sadegh Tabas, (i)removing Pysolar as tisr would be calc through GC, (ii) add NOMADS option for downloading data, (iii) add 37 pressure levels, (iv) configurations for hera
    -20240205: Sadegh Tabas, add 37 pressure levels, update s3 bucket
    -20240425: Sadegh Tabas, (i) update s3 bucket resource, 
'''
import os
import sys
from time import time
import glob
import argparse
import subprocess
from datetime import datetime, timedelta
import re
import xarray as xr
import requests
from bs4 import BeautifulSoup

# https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.20250910/18/atmos/gfs.t18z.pgrb2.0p25.f000

class DataProcessor:
    def __init__(self, forecast_day, forecast_run, output_directory=None, download_directory=None, download_pairs=True):
        self.forecast_day = forecast_day
        self.forecast_run = forecast_run
        self.output_directory = output_directory
        self.download_directory = download_directory
        self.download_pairs = download_pairs

        if not datetime.strptime(self.forecast_day, "%Y%m%d"):
            return f"Forecast Day {self.forecast_day} could not be converted to format YYYYMMDD"

        if self.forecast_run not in ["00", "06", "12", "18"]:
            return f"Forecast Run ({forecast_run}) is not 00, 06, 12, or 18"

        if self.download_directory is None:
            self.local_base_download_directory = os.path.join(os.getcwd(), "downloads")
            os.makedirs(self.local_base_download_directory, exist_ok=True)
            self.download_directory = self.local_base_download_directory
        else:
            os.makedirs(self.download_directory, exist_ok=True)

        if self.output_directory is None:
            self.local_base_output_directory = os.path.join(os.getcwd(), "outputs")
            os.makedirs(self.local_base_output_directory, exist_ok=True)
            self.output_directory = self.local_base_output_directory
        else:
            os.makedirs(self.output_directory, exist_ok=True)

        self.forecast_hours = [f"f{h:03d}" for h in range(0, 12)]
        self.base_url = f"https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.{self.forecast_day}/{self.forecast_run}/atmos/"
        self.file_base = f"gfs.t{self.forecast_run}z.pgrb2.0p25"


    def download_data(self, forecast_hour):
        base_file = f'{self.file_base}.{forecast_hour}'

        response = requests.get(self.base_url)
        if response.status_code == 200:
            # Parse the HTML content using BeautifulSoup
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find all anchor tags (links) in the HTML
            anchor_tags = soup.find_all('a')
            
            # Extract file URLs from href attributes of anchor tags
            file_urls = [self.base_url + tag['href'] for tag in anchor_tags if tag.get('href')]

            for file_url in file_urls: 
                if file_url.endswith(base_file):
                    
                    # Define the local file path
                    local_file_path = os.path.join(self.download_directory, base_file)
                    
                    # Download the file from Nomads to the local path
                    try:
                        cmd = ['wget', '-qO', local_file_path, file_url]
                        subprocess.run(cmd, check=True)
                    except subprocess.CalledProcessError as e:
                        print(f"Error downloading {file_url}: {e}")


    def process_data(self, forecast_hour):
        file_name = f'{self.file_base}.{forecast_hour}'

        variables_to_extract[file_name] = {
            ':LAND:': {
                'levels': [':surface:'],
                'first_time_step_only': True,  # Extract only the first time step
            },
            ':HGT:': {
                'levels': [':surface:'],
                'first_time_step_only': True,  # Extract only the first time step
            },
            ':TMP:': {
                'levels': [':2 m above ground:'],
            },
            ':PRMSL:': {
                'levels': [':mean sea level:'],
            },
            ':VGRD|UGRD:': {
                'levels': [':10 m above ground:'],
            },
            ':SPFH|VVEL|VGRD|UGRD|HGT|TMP:': {
                'levels': [':(50|100|150|200|250|300|400|500|600|700|850|925|1000) mb:'],
            }
        }

        extracted_datasets = []
        files = []
        
        if os.path.exists(os.path.join(self.download_directory, file_name)):
            for file_name, variable_data in variables_to_extract.items():
                for variable, data in variable_data.items():
                    levels = data['levels']
                    first_time_step_only = data.get('first_time_step_only', False)  # Default to False if not specified

                    matching_files = glob.glob(os.path.join(self.download_directory, file_name))
                    
                    if len(matching_files) == 1:
                        grib2_file = matching_files[0]
                        print("Found file:", grib2_file)
                    else:
                        print(f"Error: Found multiple or no matching files: ({len(matching_files)}). Matching Files: {matching_files}.")
                        
                    # Extract the specified variables with levels from the GRIB2 file
                    for level in levels:
                        output_file = f'{variable}_{level}_{self.forecast_day}_{self.forecast_run}Z_{forecast_hour}_13.nc'
                        files.append(output_file)
                        
                        # Extracting levels using regular expression
                        matches = re.findall(r'\d+', level)
                        
                        # Convert the extracted matches to integers
                        curr_levels = [int(match) for match in matches]
                        
                        # Get the number of levels
                        number_of_levels = len(curr_levels)
                        
                        # Use wgrib2 to extract the variable with level
                        wgrib2_command = ['wgrib2', '-nc_nlev', f'{number_of_levels}', grib2_file, '-match', f'{variable}', '-match', f'{level}', '-netcdf', output_file]
                        subprocess.run(wgrib2_command, check=True)

                        # Open the extracted netcdf file as an xarray dataset
                        ds = xr.open_dataset(output_file)

                        # If specified, extract only the first time step
                        if variable not in [':LAND:', ':HGT:']:
                            extracted_datasets.append(ds)
                        else:
                            if first_time_step_only:
                                # Append the dataset to the list
                                ds = ds.isel(time=0)
                                extracted_datasets.append(ds)
                                variables_to_extract[file_name][variable]['first_time_step_only'] = False
                        
                        # Optionally, remove the intermediate GRIB2 file
                        # os.remove(output_file)

        print("Merging grib2 files:")
        # ds = xr.merge(extracted_datasets) [OLD FORMAT - FAILS WITH NEWER XARRAY]
        ds = xr.combine_by_coords(
            extracted_datasets,
            combine_attrs="drop_conflicts",
            join="outer"
        )
        
        print("Merging process completed.")

        print("Processing, Renaming and Reshaping the data")
        # Drop the 'level' dimension
        ds = ds.drop_dims('level')

        # Rename variables and dimensions
        ds = ds.rename({
            'latitude': 'lat',
            'longitude': 'lon',
            'plevel': 'level',
            'HGT_surface': 'geopotential_at_surface',
            'LAND_surface': 'land_sea_mask',
            'PRMSL_meansealevel': 'mean_sea_level_pressure',
            'TMP_2maboveground': '2m_temperature',
            'UGRD_10maboveground': '10m_u_component_of_wind',
            'VGRD_10maboveground': '10m_v_component_of_wind',
            'APCP_surface': 'total_precipitation_6hr',
            'HGT': 'geopotential',
            'TMP': 'temperature',
            'SPFH': 'specific_humidity',
            'VVEL': 'vertical_velocity',
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
        
        # Define the output NetCDF file
        date = (self.start_datetime + timedelta(hours=6)).strftime('%Y%m%d%H')
        steps = str(len(ds['time']))
        
        if forecast_hours is None:
            fh_suffix = ""
        else:
            fh_suffix = f"_fh-{'_'.join(forecast_hours)}"

        if self.output_directory is None:
            self.output_directory = os.getcwd()  # Use current directory if not specified
        
        processed_dir = os.path.join(self.output_directory, "processed_netcdfs")

        os.makedirs(processed_dir, exist_ok=True)
        output_netcdf = os.path.join(processed_dir, f"source-gdas_date-{date}_res-0.25_levels-{self.num_levels}_steps-{steps}{fh_suffix}.nc")

        # Save the merged dataset as a NetCDF file
        ds.to_netcdf(output_netcdf)
        print(f"Saved output to {output_netcdf}")
        for file in files:
            os.remove(file)
            
        print(f"Process completed successfully, your inputs for GraphCast model generated at:\n {output_netcdf}")
        return output_netcdf

    def process_pairs(self, forecast_a, forecast_b):
        output_a = self.process_data(forecast_a)
        output_b = self.process_data(forecast_b)

        ds_a = xr.open_dataset(output_a)
        ds_b = xr.open_dataset(output_b)

        # Ensuring both datasets are on common coordinates (They should be)
        ds_a, ds_b = xr.align(ds_a, ds_b, join="exact")

        merged = xr.concat([ds_a, ds_b], dim="time")

        merged = merged.sortby("time")

        merged_dir = os.path.join(self.output_directory, "merged_forecasts")
        os.makedirs(merged_dir, exist_ok=True)

        merged_file = os.path.join(
            merged_dir,
            f"merged_{forecast_a}_{forecast_b}.nc"
        )

        merged.to_netcdf(merged_file)

        print(f"Merged dataset saved to: {merged_file}")
        return merged_file

    def start(self):

        for forecast_hour in self.forecast_hours:
            print(f"Downloading {forecast_hour}...")
            self.download_data(forecast_hour)
        
        print(f"Downloaded all {len(self.forecast_hours)} Forecast Hours")

        if self.download_pairs:
            print("Processing Pairs")
            forecast_pairs = [
                (self.forecast_hours[i], self.forecast_hours[i + 5])
                for i in range(len(self.forecast_hours) - 5)
            ]
            for forecast_a, forecast_b in forecast_pairs:
                print(f"Starting Processing of Merged Pairs: ({forecast_a}, {forecast_b})")
                output = self.process_pairs(forecast_a, forecast_b)
                print(f"Merged pair {forecast_a} + {forecast_b} saved to {output}")
        else:
            print("Processing Individual Forecasts")
            for forecast in self.forecast_hours:
                print("Started Processing Individual Forecast")
                output = self.process_data(forecast)
                print(f"Processed {forecast} saved to {output}")

        


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download and process GDAS data")
    parser.add_argument("download_date", help="Start datetime in the format 'YYYYMMDDHH'")
    parser.add_argument("-r", "--run", help="The HH value that you want to download")
    parser.add_argument("-o", "--output", help="Output directory for processed data")
    parser.add_argument("-d", "--download", help="Download directory for raw data")
    parser.add_argument("-p", "--pair", help="Write six 2-step files per cycle: (f000,f006)..(f005,f011)", default="true")

    args = parser.parse_args()

    download_date = datetime.strptime(args.download_date, "%Y%m%d%H")
    forecast_run = args.run
    num_pressure_levels = int(args.levels)
    output_directory = args.output
    download_directory = args.download
    download_pairs = args.pair.lower()

    data_processor = DataProcessor(download_date, forecast_run, output_directory, download_directory, download_pairs)
    
    data_processor.start()

