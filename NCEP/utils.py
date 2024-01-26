"""A collection of utilily functions.

    Functions:
        save_grib2: This function reads data from a netcdf file and save them as grib2 format.

    History:
        01/26/2024: Linlin Cui (linlin.cui@noaa.gov), added function save_grib2 
"""

from datetime import datetime, timedelta
import cf_units
import iris

ATTR_MAPS = {
    '10m_u_component_of_wind': [10, 'x_wind', 'm s**-1'],
    '10m_v_component_of_wind': [10, 'x_wind', 'm s**-1'],
    'mean_sea_level_pressure': [0, 'air_pressure_at_sea_level', 'Pa'],
    '2m_temperature': [2, 'air_temperature', 'K'],
    'total_precipitation_6hr': [0, 'precipitation_amount', 'kg m**-2'],
    'vertical_velocity': [None, 'lagrangian_tendency_of_air_pressure', 'Pa s**-1'],
    'specific_humidity': [None, 'specific_humidity', 'kg kg**-1'],
    'temperature': [None, 'air_temperature', 'K'],
    'geopotential': [None, 'geopotential_height', 'm'],
    'u_component_of_wind': [None, 'x_wind', 'm s**-1'],
    'v_component_of_wind': [None, 'y_wind', 'm s**-1'],
}


def save_grib2(start_datetime, filename):
    """Convert netcdf file to grib2 format file

    Args:
      start_datetime: datetime object, which specify model's start time
      filename: file name, including the path

    Returns:
      No return values, will save to grib2 file
    """

    cubes = iris.load(filename)
    times = cubes[0].coord('time').points
    datevectors = [start_datetime + timedelta(hours=int(t)) for t in times]

    time_fmt_str = '00:00:00'
    time_unit_str = f"Hours since {start_datetime.strftime('%Y-%m-%d %H:00:00')}"
    time_coord = cubes[0].coord('time')
    new_time_unit = cf_units.Unit(time_unit_str, calendar=cf_units.CALENDAR_STANDARD)
    new_time_points = [new_time_unit.date2num(dt) for dt in datevectors]
    new_time_coord = iris.coords.DimCoord(new_time_points, standard_name='time', units=new_time_unit)

    for date in datevectors:
        print(f"Processing for time {date.strftime('%Y-%m-%d %H:00:00')}")
        hrs = int((date - start_datetime).total_seconds() // 3600)
        print(hrs)
    
        new_list = []
    
        for cube in cubes:
            var_name = cube.name()
        
            time_coord_dim = cube.coord_dims('time')
            cube.remove_coord('time')
            cube.add_dim_coord(new_time_coord, time_coord_dim)
            
            hour_6 = iris.Constraint(time=iris.time.PartialDateTime(month=date.month, day=date.day, hour=date.hour))
            cube_slice = cube.extract(hour_6)
            cube_slice.coord('latitude').coord_system=iris.coord_systems.GeogCS(4326)
            cube_slice.coord('longitude').coord_system=iris.coord_systems.GeogCS(4326)
    
            if len(cube_slice.data.shape) == 3:
                levels = cube_slice.coord('pressure').points
                for level in levels:
                    cube_slice_level = cube_slice.extract(iris.Constraint(pressure=level))
                    cube_slice_level.add_aux_coord(iris.coords.DimCoord(hrs, standard_name='forecast_period', units='hours'))
                    cube_slice_level.standard_name = ATTR_MAPS[var_name][1]
                    cube_slice_level.units = ATTR_MAPS[var_name][2]
                    cube_slice_level.long_name = var_name
                    new_list.append(cube_slice_level)
            else:
                cube_slice.add_aux_coord(iris.coords.DimCoord(hrs, standard_name='forecast_period', units='hours'))
    
                if var_name not in ['mean_sea_level_pressure', 'total_precipitation_6hr']:
                    cube_slice.add_aux_coord(iris.coords.DimCoord(ATTR_MAPS[var_name][0], standard_name='height', units='m'))
                #TODO: figure out how to set levelType to meanSea
                #if var_name == 'mean_sea_level_pressure':
                #    cube_slice.add_aux_coord(iris.coords.DimCoord(ATTR_MAPS[var_name][0], standard_name='height', units='m'))
    
                #This part is not working right now, may look into eccodes sourcecode
                if var_name == 'total_precipitation_6hr':
                    cube_slice.stepType = 'accum'
                    cube_slice.stepRange = f'{hrs-6}-{hrs}'
                    cube_slice.stepUnits = 1
                    cube_slice.step = 6
                    cube_slice.startStep = hrs - 6
                    cube_slice.endStep = hrs
    
                cube_slice.standard_name = ATTR_MAPS[var_name][1]
                cube_slice.units = ATTR_MAPS[var_name][2]
                cube_slice.long_name = var_name
                new_list.append(cube_slice)
        
        cycle = start_datetime.hour
        iris.save(new_list, f'graphcast.t{cycle:02d}z.f{hrs:03d}.grib2')
