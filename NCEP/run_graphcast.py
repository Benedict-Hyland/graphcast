'''
Description: Script to call the graphcast model using gdas products
Author: Sadegh Sadeghi Tabas (sadegh.tabas@noaa.gov)
Revision history:
    -20231218: Sadegh Tabas, initial code
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

    def load_pretrained_model(self, pretrained_model_path):
        """Load pre-trained GraphCast model."""
        with open(pretrained_model_path, "rb") as f:
            ckpt = checkpoint.load(f, graphcast.CheckPoint)
            self.params = ckpt.params
            self.state = {}
            self.model_config = ckpt.model_config
            self.task_config = ckpt.task_config

    def load_gdas_data(self, gdas_data_path):
        """Load GDAS data."""
        #with open(gdas_data_path, "rb") as f:
        #    self.current_batch = xarray.load_dataset(f).compute()
        self.current_batch = xarray.load_dataset(gdas_data_path).compute()
        assert self.current_batch.dims["time"] == 42

    def extract_inputs_targets_forcings(self):
        """Extract inputs, targets, and forcings from the loaded data."""
        self.inputs, self.targets, self.forcings = data_utils.extract_inputs_targets_forcings(
            self.current_batch, target_lead_times=slice("6h", f"{40*6}h"), **dataclasses.asdict(self.task_config)
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
    
 
    def get_predictions(self, fname):
        """Run GraphCast and save forecasts to a NetCDF file."""
        
        self.load_model()
            
        # output = self.model(self.model ,rng=jax.random.PRNGKey(0), inputs=self.inputs, targets_template=self.targets * np.nan, forcings=self.forcings,)
        forecasts = rollout.chunked_prediction(self.model ,rng=jax.random.PRNGKey(0), inputs=self.inputs, targets_template=self.targets * np.nan, forcings=self.forcings,)
        
        # save forecasts
        forecasts.to_netcdf(f"forecasts/gc_forecasts_{fname}.nc")




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run GraphCast prediction.")
    parser.add_argument("-i", "--input", help="Input filename", required=True)
    parser.add_argument("-o", "--output", help="Output filename", required=True)
    args = parser.parse_args()

    runner = GraphCastModel()
    runner.load_pretrained_model("/contrib/graphcast/NCEP/params/GraphCast_operational - ERA5-HRES 1979-2021 - resolution 0.25 - pressure levels 13 - mesh 2to6 - precipitation output only.npz")
    runner.load_gdas_data(args.input)
    runner.extract_inputs_targets_forcings()
    runner.load_normalization_stats(
        "/contrib/graphcast/NCEP/stats/diffs_stddev_by_level.nc", 
        "/contrib/graphcast/NCEP/stats/mean_by_level.nc", 
        "/contrib/graphcast/NCEP/stats/stddev_by_level.nc"
    )
    runner.get_predictions(args.output)
