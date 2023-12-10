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

class GraphCastRunner:
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
        with open(gdas_data_path, "rb") as f:
            self.current_batch = xarray.load_dataset(f).compute()
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

    def construct_wrapped_graphcast(self):
        """Construct and wrap the GraphCast predictor."""
        predictor = graphcast.GraphCast(self.model_config, self.task_config)
        predictor = casting.Bfloat16Cast(predictor)
        predictor = normalization.InputsAndResiduals(
            predictor, diffs_stddev_by_level=self.diffs_stddev_by_level,
            mean_by_level=self.mean_by_level, stddev_by_level=self.stddev_by_level
        )
        predictor = autoregressive.Predictor(predictor, gradient_checkpointing=True)
        return predictor

    def run_forward(self):
        """Run the forward pass."""
        predictor = self.construct_wrapped_graphcast()
        return predictor(self.inputs, targets_template=self.targets * np.nan, forcings=self.forcings)

    def save_predictions(self, fname):
        """Save predictions to a NetCDF file."""
        predictions = rollout.chunked_prediction(
            jax.jit(self.run_forward), rng=jax.random.PRNGKey(0), inputs=self.inputs,
            targets_template=self.targets * np.nan, forcings=self.forcings
        )
        predictions.to_netcdf(f"gc_preds_{fname}.nc")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run GraphCast prediction.")
    parser.add_argument("-i", "--input", help="Input filename", required=True)
    parser.add_argument("-o", "--output", help="Output filename", required=True)
    args = parser.parse_args()

    runner = GraphCastRunner()
    runner.load_pretrained_model("params/GraphCast_operational - ERA5-HRES 1979-2021 - resolution 0.25 - pressure levels 13 - mesh 2to6 - precipitation output only.npz")
    runner.load_gdas_data(args.input)
    runner.extract_inputs_targets_forcings()
    runner.load_normalization_stats(
        "stats/diffs_stddev_by_level.nc", "stats/mean_by_level.nc", "stats/stddev_by_level.nc"
    )
    runner.save_predictions(args.output)
