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
from time import time, sleep
import glob
import argparse
import subprocess
from datetime import datetime, timedelta
import re
import xarray as xr
import requests
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd

# Only one file allowed to be open to avoid bloat issues
# xr.set_options(file_cache_maxsize=1)

# https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.20250910/18/atmos/gfs.t18z.pgrb2.0p25.f000

print("\nUsing latest GDAS utility script\n")

class DataProcessor:
    def __init__(self, forecast_day, forecast_run, output_directory=None, download_directory=None, download_pairs=True):
        self.forecast_day = forecast_day
        self.forecast_run = forecast_run
        self.output_directory = output_directory
        self.download_directory = download_directory
        self.download_pairs = download_pairs

        try:
            self.start_datetime = datetime.strptime(self.forecast_day, "%Y%m%d")
        except ValueError:
            raise ValueError(f"Forecast Day {self.forecast_day} is not in YYYYMMDD format.")

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

        self.forecast_hours = [f"f{h:03d}" for h in range(0, 12+1)]
        self.base_url = f"https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.{self.forecast_day}/{self.forecast_run}/atmos/"
        self.file_base = f"gfs.t{self.forecast_run}z.pgrb2.0p25"


    def download_data(self, forecast_hour):
        base_file = f"{self.file_base}.{forecast_hour}"
        local_file_path = os.path.join(self.download_directory, base_file)

        # Ensure download directory exists
        os.makedirs(self.download_directory, exist_ok=True)

        # Get the listing page from NOMADS
        response = requests.get(self.base_url, timeout=30)
        if response.status_code != 200:
            raise ConnectionError(f"Could not access {self.base_url} (HTTP {response.status_code})")

        # Parse HTML and find matching GRIB file URL
        soup = BeautifulSoup(response.content, "html.parser")
        file_urls = [self.base_url + tag["href"] for tag in soup.find_all("a") if tag.get("href")]
        file_url = next((url for url in file_urls if url.endswith(base_file)), None)

        if not file_url:
            raise FileNotFoundError(f"Could not find matching GRIB2 file for {base_file}")

        # --- Start download with integrity checks ---
        max_retries = 5
        backoff = 5  # seconds
        temp_path = local_file_path + ".part"

        for attempt in range(1, max_retries + 1):
            try:
                # Get remote file size
                head = requests.head(file_url, timeout=15)
                expected_size = int(head.headers.get("Content-Length", 0))

                # Resume if partial file exists
                resume_header = {}
                pos = 0
                if os.path.exists(temp_path):
                    pos = os.path.getsize(temp_path)
                    if pos < expected_size:
                        resume_header = {"Range": f"bytes={pos}-"}
                        print(f"Resuming {base_file} from {pos:,} bytes...")
                    else:
                        # already complete
                        os.rename(temp_path, local_file_path)
                        print(f"{base_file} already fully downloaded.")
                        return

                # Start streaming download
                with requests.get(file_url, headers=resume_header, stream=True, timeout=60) as r:
                    r.raise_for_status()
                    mode = "ab" if resume_header else "wb"
                    with open(temp_path, mode) as f:
                        for chunk in r.iter_content(chunk_size=1024 * 1024):
                            if not chunk:
                                continue
                            f.write(chunk)

                # Verify integrity
                final_size = os.path.getsize(temp_path)
                if expected_size and final_size < expected_size:
                    raise IOError(f"Incomplete download ({final_size}/{expected_size} bytes)")

                # Rename to final name only when successful
                os.rename(temp_path, local_file_path)
                print(f"✅ Downloaded {base_file} ({final_size/1e6:.1f} MB)")
                return

            except Exception as e:
                print(f"⚠️ Download failed (attempt {attempt}/{max_retries}): {e}")
                if attempt < max_retries:
                    print(f"Retrying in {backoff} seconds...")
                    sleep(backoff)
                    backoff *= 2
                else:
                    raise RuntimeError(f"Failed to download {base_file} after {max_retries} attempts")

        # If we exit loop without success
        raise RuntimeError(f"Failed to download {base_file}")


    def process_data(self, forecast_hour):
        file_name = f'{self.file_base}.{forecast_hour}'

        variables_to_extract = {
            file_name: {
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
        }

        extracted_datasets = []
        files = []
        
        grib_path = os.path.join(self.download_directory, file_name)
        if not os.path.exists(grib_path):
            raise FileNotFoundError(f"Expected GRIB file missing: {grib_path}")

        for grib_file, variable_data in variables_to_extract.items():
            matching_files = glob.glob(os.path.join(self.download_directory, grib_file))
            if len(matching_files) != 1:
                raise RuntimeError(f"Found multiple or no matching files ({len(matching_files)}): {matching_files}")
            
            grib2_file = matching_files[0]
            print("Found file:", grib2_file)

            for variable, data in variable_data.items():
                levels = data['levels']
                first_time_step_only = data.get('first_time_step_only', False)  # Default to False if not specified
                
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
                    subprocess.run(wgrib2_command, check=True, stdout=subprocess.DEVNULL)

                    # Open the extracted netcdf file as an xarray dataset
                    ds_part = xr.open_dataset(output_file)

                    # If specified, extract only the first time step
                    if variable in [':LAND:', ':HGT:'] and first_time_step_only:
                        ds_part = ds_part.isel(time=0)
                        variables_to_extract[grib_file][variable]['first_time_step_only'] = False
                    
                    extracted_datasets.append(ds_part)
                    # Optionally, remove the intermediate GRIB2 file
                    # os.remove(output_file)

        # print("Combining grib2 files:")
        # template_lat = np.round(np.arange(-90, 90.25, 0.25), 3)
        # template_lon = np.round(np.arange(0, 360, 0.25), 3)
        # ds = ds.assign_coords(lat=template_lat, lon=template_lon)
        ds = xr.merge(
            extracted_datasets,
            combine_attrs="drop_conflicts",
            join="outer",
            compat="override"
        ).sortby("time")

        # ds = xr.combine_by_coords(
        #     extracted_datasets,
        #     combine_attrs="drop_conflicts",
        #     join="outer",
        #     compat="override"
        # )

        # 1) absolute datetimes along time (datetime64[ns])
        abs_time = pd.to_datetime(ds["time"].values)
        ds = ds.assign_coords(datetime=("time", abs_time))

        # 2) time as 0-based timedelta64 starting at 0, stepping 6h
        ds = ds.assign_coords(time=("time", (abs_time - abs_time[0]).astype("timedelta64[ns]")))

        # 3) batch dimension: make datetime shape [batch, time]
        ds = ds.expand_dims("batch")
        ds["datetime"] = ds["datetime"].expand_dims("batch")

        print("Processing, Renaming and Reshaping the data")
        # Drop the 'level' dimension
        if "level" in ds.dims:
            ds = ds.drop_dims("level")

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
            'HGT': 'geopotential',
            'TMP': 'temperature',
            'SPFH': 'specific_humidity',
            'VVEL': 'vertical_velocity',
            'UGRD': 'u_component_of_wind',
            'VGRD': 'v_component_of_wind'
        })

        # Assign 'datetime' as coordinates
        # ds = ds.assign_coords(datetime=ds.time)
        
        # Convert data types
        if 'lat' in ds:   ds['lat']   = ds['lat'].astype('float32')
        if 'lon' in ds:   ds['lon']   = ds['lon'].astype('float32')
        if 'level' in ds: ds['level'] = ds['level'].astype('int32')

        # Adjust time values relative to the first time step
        # ds["time"] = ds["time"] - ds.time[0]

        # Expand dimensions
        # ds = ds.expand_dims(dim='batch')
        # ds['datetime'] = ds['datetime'].expand_dims(dim='batch')

        # Squeeze dimensions
        # ds['geopotential_at_surface'] = ds['geopotential_at_surface'].squeeze('batch')
        # ds['land_sea_mask'] = ds['land_sea_mask'].squeeze('batch')

        # Update geopotential unit to m2/s2 by multiplying 9.80665
        ds['geopotential_at_surface'] = ds['geopotential_at_surface'] * 9.80665
        ds['geopotential'] = ds['geopotential'] * 9.80665

        # Sanity Checks
        assert str(ds.time.dtype).startswith("timedelta64")
        assert str(ds.datetime.dtype).startswith("datetime64")
        assert 'batch' in ds.dims and ds['datetime'].ndim == 2
        assert not pd.isna(ds['datetime']).any()
        
        if self.output_directory is None:
            self.output_directory = os.getcwd()  # Use current directory if not specified
        
        processed_dir = os.path.join(self.output_directory, "processed_netcdfs")

        os.makedirs(processed_dir, exist_ok=True)
        output_netcdf = os.path.join(processed_dir, f"{forecast_hour}.nc")

        # Save the merged dataset as a NetCDF file
        ds.to_netcdf(output_netcdf)
        print(f"Saved output to {output_netcdf}")
        for file in files:
            ds.close()
            # os.remove(file)
            
        print(f"Process completed successfully, your inputs for GraphCast model generated at:\n {output_netcdf}")
        return output_netcdf

    @staticmethod
    def _ensure_normalized(ds: xr.Dataset) -> xr.Dataset:
        """Verify and coerce GraphCast-required time/datetime/batch structure."""
        ds = ds.sortby("time")

        # If time is datetime-like, convert to timedelta starting at 0
        if str(ds.time.dtype).startswith("datetime64"):
            abs_time = pd.to_datetime(ds["time"].values)
            ds = ds.assign_coords(datetime=("time", abs_time))
            ds = ds.assign_coords(time=("time", (abs_time - abs_time[0]).astype("timedelta64[ns]")))
        elif "datetime" not in ds.coords:
            # Build datetime from a nominal origin if missing (shouldn't happen if saved by process_data)
            abs_time = pd.Timestamp("1970-01-01") + pd.to_timedelta(ds["time"].values)
            ds = ds.assign_coords(datetime=("time", abs_time))

        # Ensure batch dim and 2D datetime
        if "batch" not in ds.dims:
            ds = ds.expand_dims("batch")
        if ds["datetime"].ndim == 1:
            ds["datetime"] = ds["datetime"].expand_dims("batch")

        # Sanity
        assert str(ds.time.dtype).startswith("timedelta64")
        assert str(ds.datetime.dtype).startswith("datetime64")
        assert 'batch' in ds.dims and ds['datetime'].ndim == 2
        assert not pd.isna(ds['datetime']).any()

        return ds

    def process_pairs(self, forecast_a, forecast_b):
        output_a = self.process_data(forecast_a)
        output_b = self.process_data(forecast_b)

        ds_a = xr.open_dataset(output_a)
        ds_b = xr.open_dataset(output_b)

        # Ensure both datasets have GraphCast-compatible structure
        ds_a = self._ensure_normalized(ds_a)
        ds_b = self._ensure_normalized(ds_b)

        #  Shift the second chunk’s time by +6 hours so the merged axis is [0h, 6h]
        #  Also shift the absolute datetime to remain consistent with the new lead-times.
        shift = np.timedelta64(6, "h")
        ds_b = ds_b.assign_coords(time=ds_b.time + shift)
        if "datetime" in ds_b.coords:
            # datetime is 2D [batch, time]; add the same shift along time
            dt_b = ds_b["datetime"].values
            ds_b = ds_b.assign_coords(datetime=(
                ("batch", "time"), (dt_b.astype("datetime64[ns]") + shift)
            ))

        # Explicit concat options to be forward-compatible with xarray defaults
        merged = xr.concat(
            [ds_a, ds_b],
            dim="time",
            data_vars="all",      # include all data variables
            coords="minimal",     # only concatenate along the time coord
            compat="no_conflicts",# allow identical-or-missing metadata
            join="outer",         # default for concat; explicit to avoid warnings
        ).sortby("time")

        # Final sanity checks
        assert str(merged.time.dtype).startswith("timedelta64")
        assert str(merged.datetime.dtype).startswith("datetime64")
        assert 'batch' in merged.dims and merged['datetime'].ndim == 2
        assert not pd.isna(merged['datetime']).any()
        # Strictly increasing time
        tvals = merged.time.values
        assert (tvals[1:] - tvals[:-1] > np.timedelta64(0, 'ns')).all()

        # Rebuild a clean 2D datetime grid from the earliest absolute timestamp
        # so downstream reindexing in run_graphcast.py can extend it deterministically
        # without producing NaT values.
        # Assumes regular 6-hourly cadence, which is true for GFS/GraphCast inputs.
        start_dt = pd.Timestamp(merged["datetime"].values[0, 0])
        ntime = merged.sizes["time"]
        step = pd.to_timedelta(np.arange(ntime) * 6, unit="h").values.astype("timedelta64[ns]")
        dt_1d = (start_dt.to_datetime64() + step).astype("datetime64[ns]")
        # Broadcast to [batch, time]
        nbatch = merged.sizes.get("batch", 1)
        dt_2d = np.broadcast_to(dt_1d, (nbatch, ntime))
        merged = merged.assign_coords(datetime=(("batch", "time"), dt_2d))

        # Ensure time encoding uses hours to avoid timedelta 'days' warning on write
        try:
            merged["time"].encoding.update({"units": "hours"})
        except Exception:
            pass

        merged_dir = os.path.join(self.output_directory, "merged_forecasts")
        os.makedirs(merged_dir, exist_ok=True)

        merged_file = os.path.join(
            merged_dir,
            f"merged_{forecast_a}_{forecast_b}.nc"
        )

        merged.to_netcdf(merged_file)
        print(f"Merged dataset saved to: {merged_file}")

        # Close
        ds_a.close()
        ds_b.close()
        merged.close()

        return merged_file


    def start(self):

        for forecast_hour in self.forecast_hours:
            print(f"Downloading {forecast_hour}...")
            self.download_data(forecast_hour)
        
        print(f"Downloaded all {len(self.forecast_hours)} Forecast Hours")

        if self.download_pairs:
            print("Processing Pairs")
            forecast_pairs = [
                (self.forecast_hours[i], self.forecast_hours[i + 6])
                for i in range(len(self.forecast_hours) - 6)
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
    parser.add_argument("download_date", help="Start datetime in the format 'YYYYMMDD'")
    parser.add_argument("-r", "--run", help="The HH value that you want to download")
    parser.add_argument("-o", "--output", help="Output directory for processed data")
    parser.add_argument("-d", "--download", help="Download directory for raw data")
    parser.add_argument("-p", "--pair", help="Write six 2-step files per cycle: (f000,f006)..(f005,f011)", default="true")

    args = parser.parse_args()

    download_date = args.download_date
    forecast_run = args.run
    output_directory = args.output
    download_directory = args.download
    download_pairs = str(args.pair).lower() in ("true", "1", "yes", "y", "t")

    data_processor = DataProcessor(download_date, forecast_run, output_directory, download_directory, download_pairs)
    
    data_processor.start()
