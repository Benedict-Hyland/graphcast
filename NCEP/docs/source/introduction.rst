######################
GraphCast Global Forecast System (GraphCast GFS)    
######################

**The GraphCastGFS system** is a weather forecast model built upon the pre-trained Google DeepMind's GraphCast Machine Learning Weather Prediction (MLWP) model. It is set up by the National Centers for Environmental Prediction (NCEP) to produce medium range global forecasts. The model runs in two operation modes on different vertical resolutions: 13 and 37 pressure levels. The horizontal resolution is a 0.25 degree latitude-longitude grid (about 28 km). The model runs 4 times a day at 00Z, 06Z, 12Z, and 1Z cycles. Major surface and atmospheric fields including temperature, wind components, geopotential height, specific humidity, and vertical velocity are available. The products are 6-hourly forecasts up to 10 days.

The Google DeepMind's GraphCast model is implemented as a message passing graph neural network (GNN) architecture with "encoder-processor-decoder" configuration. It uses an icosahedron grid with multiscale edges and has around 37 milion parameters. The model is pre-trained with ECMWF's ERA5 reanalysis data. The GraphCastGFS model takes two model states as initial conditions (current and 6-hr previous states) from NCEP 0.25 degree GDAS analysis data. 
