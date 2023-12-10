# import dependencies
import dataclasses
import datetime
import functools
import math
import re
from typing import Optional
# from IPython.display import HTML
import ipywidgets as widgets
import haiku as hk
import jax
# import matplotlib
# import matplotlib.pyplot as plt
# from matplotlib import animation
import numpy as np
import xarray
import cartopy.crs as ccrs

# import graphcast
from graphcast import autoregressive
from graphcast import casting
from graphcast import checkpoint
from graphcast import data_utils
from graphcast import graphcast
from graphcast import normalization
from graphcast import rollout
from graphcast import xarray_jax
from graphcast import xarray_tree

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
    
