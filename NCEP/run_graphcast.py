# import dependencies
import dataclasses
# import datetime
import functools
import math
import re
# from typing import Optional
# from IPython.display import HTML
# import ipywidgets as widgets
import haiku as hk
import jax
# import matplotlib
# import matplotlib.pyplot as plt
# from matplotlib import animation
import numpy as np
import xarray
# import cartopy.crs as ccrs

# import graphcast
from graphcast import autoregressive
from graphcast import casting
from graphcast import checkpoint
from graphcast import data_utils
from graphcast import graphcast
from graphcast import normalization
from graphcast import rollout
# from graphcast import xarray_jax
# from graphcast import xarray_tree

# load pre-trained model
with open("params/GraphCast_operational - ERA5-HRES 1979-2021 - resolution 0.25 - pressure levels 13 - mesh 2to6 - precipitation output only.npz", "rb") as f:
    ckpt = checkpoint.load(f, graphcast.CheckPoint)
    params = ckpt.params
    state = {}
    model_config = ckpt.model_config
    task_config = ckpt.task_config

# open gdas states file (curr step and 6-hour earlier) as well as forcings (TOA Incident Solar Radiation)
with open('source-gdas_date-20220101_res-0.25_levels-13_steps-40.nc',"rb") as f:
    current_batch = xarray.load_dataset(f).compute()
assert current_batch.dims["time"] == 42

# getting inputs, targets (NaN) and forcings using gc.data_utils
inputs, targets, forcings = data_utils.extract_inputs_targets_forcings(current_batch, target_lead_times=slice("6h", f"{40*6}h"), **dataclasses.asdict(task_config))

# Load normalization stats
with open('stats/diffs_stddev_by_level.nc',"rb") as f:
    diffs_stddev_by_level = xarray.load_dataset(f).compute()
with open("stats/mean_by_level.nc", "rb") as f:
    mean_by_level = xarray.load_dataset(f).compute()
with open("stats/stddev_by_level.nc","rb") as f:
    stddev_by_level = xarray.load_dataset(f).compute()
    
# Build jitted functions, and initialize weights
def construct_wrapped_graphcast(model_config: graphcast.ModelConfig, task_config: graphcast.TaskConfig):
    """Constructs and wraps the GraphCast Predictor."""
    # Deeper one-step predictor.
    predictor = graphcast.GraphCast(model_config, task_config)

    # Modify inputs/outputs to `graphcast.GraphCast` to handle conversion to
    # from/to float32 to/from BFloat16.
    predictor = casting.Bfloat16Cast(predictor)

    # Modify inputs/outputs to `casting.Bfloat16Cast` so the casting to/from
    # BFloat16 happens after applying normalization to the inputs/targets.
    predictor = normalization.InputsAndResiduals(predictor, diffs_stddev_by_level=diffs_stddev_by_level, mean_by_level=mean_by_level, stddev_by_level=stddev_by_level)

    # Wraps everything so the one-step model can produce trajectories.
    predictor = autoregressive.Predictor(predictor, gradient_checkpointing=True)
    return predictor


@hk.transform_with_state
def run_forward(model_config, task_config, inputs, targets_template, forcings):
    predictor = construct_wrapped_graphcast(model_config, task_config)
    return predictor(inputs, targets_template=targets_template, forcings=forcings)

# Jax doesn't seem to like passing configs as args through the jit. Passing it
# in via partial (instead of capture by closure) forces jax to invalidate the
# jit cache if you change configs.
def with_configs(fn):
    return functools.partial(fn, model_config=model_config, task_config=task_config)

# Always pass params and state, so the usage below are simpler
def with_params(fn):
    return functools.partial(fn, params=params, state=state)

# Our models aren't stateful, so the state is always empty, so just return the
# predictions. This is requiredy by our rollout code, and generally simpler.
def drop_state(fn):
    return lambda **kw: fn(**kw)[0]

init_jitted = jax.jit(with_configs(run_forward.init))

run_forward_jitted = drop_state(with_params(jax.jit(with_configs(run_forward.apply))))

# calc graphcast forecasts for 40 timesteps (10 days) using rollout func
predictions = rollout.chunked_prediction(run_forward_jitted, rng=jax.random.PRNGKey(0), inputs=inputs, targets_template=targets * np.nan, forcings=forcings)
