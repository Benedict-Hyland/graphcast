'''
Description: Script to call the graphcast model using gdas products
Author: Sadegh Sadeghi Tabas (sadegh.tabas@noaa.gov)
Revision history:
    -20231218: Sadegh Tabas, initial code
    -20240118: Sadegh Tabas, S3 bucket module to upload data, adding forecast length
'''
import argparse
import dataclasses
import functools
import math
import re
import haiku as hk
import jax
import numpy as np
import xarray
import boto3
import os

from graphcast import autoregressive
from graphcast import casting
from graphcast import checkpoint
from graphcast import data_utils
from graphcast import graphcast
from graphcast import normalization
from graphcast import rollout

class GraphCastModel:
    def __init__(self):
        self.params = None
        self.state = {}
        self.model_config = None
        self.task_config = None
        self.diffs_stddev_by_level = None
        self.mean_by_level = None
        self.stddev_by_level = None
        self.current_batch = None
        self.inputs = None
        self.targets = None
        self.forcings = None
        self.s3_bucket_name = noaa-nws-graphcastgfs-pds

    def load_pretrained_model(self, pretrained_model_path):
        """Load pre-trained GraphCast model."""
        with open(pretrained_model_path, "rb") as f:
            ckpt = checkpoint.load(f, graphcast.CheckPoint)
            self.params = ckpt.params
            self.state = {}
            self.model_config = ckpt.model_config
            self.task_config = ckpt.task_config

    def load_gdas_data(self, gdas_data_path, forecast_length = 40):
        """Load GDAS data."""
        #with open(gdas_data_path, "rb") as f:
        #    self.current_batch = xarray.load_dataset(f).compute()
        self.current_batch = xarray.load_dataset(gdas_data_path).compute()
        
        if (forecast_length + 2) > len(self.current_batch['time']):
            print('Updating batch dataset to account for forecast length')
            
            diff = int(forecast_length + 2 - len(self.current_batch['time']))
            ds = self.current_batch

            # time and datetime update
            curr_time_range = ds['time'].values.astype('timedelta64[ns]')
            new_time_range = (np.arange(len(curr_time_range) + diff) * np.timedelta64(6, 'h')).astype('timedelta64[ns]')
            ds = ds.reindex(time = new_time_range)
            curr_datetime_range = ds['datetime'][0].values.astype('datetime64[ns]')
            new_datetime_range = curr_datetime_range[0] + np.arange(len(curr_time_range) + diff) * np.timedelta64(6, 'h')
            ds['datetime'][0]= new_datetime_range

            self.current_batch = ds
            print('batch dataset updated')
            
            
        
    def extract_inputs_targets_forcings(self, forecast_length = 40):
        """Extract inputs, targets, and forcings from the loaded data."""
        self.inputs, self.targets, self.forcings = data_utils.extract_inputs_targets_forcings(
            self.current_batch, target_lead_times=slice("6h", f"{forecast_length*6}h"), **dataclasses.asdict(self.task_config)
        )

    def load_normalization_stats(self, diffs_stddev_path, mean_path, stddev_path):
        """Load normalization stats."""
        with open(diffs_stddev_path, "rb") as f:
            self.diffs_stddev_by_level = xarray.load_dataset(f).compute()
        with open(mean_path, "rb") as f:
            self.mean_by_level = xarray.load_dataset(f).compute()
        with open(stddev_path, "rb") as f:
            self.stddev_by_level = xarray.load_dataset(f).compute()
    
    # Jax doesn't seem to like passing configs as args through the jit. Passing it
    # in via partial (instead of capture by closure) forces jax to invalidate the
    # jit cache if you change configs.
    def _with_configs(self, fn):
        return functools.partial(fn, model_config=self.model_config, task_config=self.task_config,)

    # Always pass params and state, so the usage below are simpler
    def _with_params(self, fn):
        return functools.partial(fn, params=self.params, state=self.state)

    # Deepmind models aren't stateful, so the state is always empty, so just return the
    # predictions. This is requiredy by the rollout code, and generally simpler.
    @staticmethod
    def _drop_state(fn):
        return lambda **kw: fn(**kw)[0]

    def load_model(self):
        def construct_wrapped_graphcast(model_config, task_config):
            """Constructs and wraps the GraphCast Predictor."""
            # Deeper one-step predictor.
            predictor = graphcast.GraphCast(model_config, task_config)

            # Modify inputs/outputs to `graphcast.GraphCast` to handle conversion to
            # from/to float32 to/from BFloat16.
            predictor = casting.Bfloat16Cast(predictor)

            # Modify inputs/outputs to `casting.Bfloat16Cast` so the casting to/from
            # BFloat16 happens after applying normalization to the inputs/targets.
            predictor = normalization.InputsAndResiduals(predictor, diffs_stddev_by_level=self.diffs_stddev_by_level, mean_by_level=self.mean_by_level, stddev_by_level=self.stddev_by_level,)

            # Wraps everything so the one-step model can produce trajectories.
            predictor = autoregressive.Predictor(predictor, gradient_checkpointing=True,)
            return predictor

        @hk.transform_with_state
        def run_forward(model_config, task_config, inputs, targets_template, forcings,):
            predictor = construct_wrapped_graphcast(model_config, task_config)
            return predictor(inputs, targets_template=targets_template, forcings=forcings,)
        
        jax.jit(self._with_configs(run_forward.init))
        self.model = self._drop_state(self._with_params(jax.jit(self._with_configs(run_forward.apply))))
    
 
    def get_predictions(self, fname, forecast_length):
        """Run GraphCast and save forecasts to a NetCDF file."""

        print (f"start running GraphCast for {forecast_length} steps --> {forecast_length*6} hours.")
        self.load_model()
            
        # output = self.model(self.model ,rng=jax.random.PRNGKey(0), inputs=self.inputs, targets_template=self.targets * np.nan, forcings=self.forcings,)
        forecasts = rollout.chunked_prediction(self.model ,rng=jax.random.PRNGKey(0), inputs=self.inputs, targets_template=self.targets * np.nan, forcings=self.forcings,)
        
        # save forecasts
        forecasts.to_netcdf(f"{fname}")

        print (f"GraphCast run completed successfully, you can find the GraphCast forecasts in the following directory:\n {fname}")

    def upload_to_s3(self, input_file, output_file, upload=False, delete_files=False):
        s3 = boto3.client('s3')

        # Extract date and time information from the input file name
        date_time_info = input_file.split('_')[2]
        date = date_time_info[:8]
        time = date_time_info[8:10]
        
        pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run GraphCast model.")
    parser.add_argument("-i", "--input", help="input file path", required=True)
    parser.add_argument("-o", "--output", help="output file path (including file name)", required=True)
    parser.add_argument("-l", "--length", help="length of forecast (6-hourly), an integer number in range [1, 40]", required=True)
    parser.add_argument("-u", "--upload", help="upload input data as well as forecasts to noaa s3 bucket (yes or no)", default = "no")
    parser.add_argument("-k", "--keep", help="keep input and output after uploading to noaa s3 bucket (yes or no)", default = "no")
    
    args = parser.parse_args()

    runner = GraphCastModel()
    runner.load_pretrained_model("/contrib/graphcast/NCEP/params/GraphCast_operational - ERA5-HRES 1979-2021 - resolution 0.25 - pressure levels 13 - mesh 2to6 - precipitation output only.npz")
    runner.load_gdas_data(args.input, int(args.length))
    runner.extract_inputs_targets_forcings(int(args.length))
    runner.load_normalization_stats(
        "/contrib/graphcast/NCEP/stats/diffs_stddev_by_level.nc", 
        "/contrib/graphcast/NCEP/stats/mean_by_level.nc", 
        "/contrib/graphcast/NCEP/stats/stddev_by_level.nc"
    )
    runner.get_predictions(args.output, int(args.length))
    runner.upload_to_s3(args.upload, args.keep)
