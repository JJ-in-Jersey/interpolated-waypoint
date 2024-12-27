from argparse import ArgumentParser as argParser
from pathlib import Path

import numpy as np
import pandas as pd
from functools import reduce
from math import ceil

from tt_noaa_data.noaa_data import StationDict
from tt_gpx.gpx import GpxFile, Route, Waypoint
from tt_file_tools.file_tools import read_df, write_df, print_file_exists

from sympy import Point
from tt_interpolation.velocity_interpolation import Interpolator as VInt
from tt_job_manager.job_manager import Job, JobManager


class InterpolatedPoint:

    def __init__(self, interpolation_pt_data, lats, lons, vels):
        num_points = range(len(vels))
        surface_points = tuple([Point(lats[i], lons[i], vels[i]) for i in num_points])
        interpolator = VInt(surface_points)
        interpolator.set_interpolation_point(Point(interpolation_pt_data[1], interpolation_pt_data[2], 0))
        interpolated_velocity = round(float(interpolator.get_interpolated_point().z.evalf()), 2)
        self.velocity = interpolated_velocity


# noinspection PyShadowingNames
class InterpolatePointJob(Job):

    def execute(self): return super().execute()

    def execute_callback(self, result): return super().execute_callback(result)
    def error_callback(self, result): return super().error_callback(result)

    def __init__(self, interpolated_pt: Waypoint, lats: list, lons: list, velos: list, timestamp: int, index: int):
        interpolated_pt_data = tuple([interpolated_pt.name, interpolated_pt.lat, interpolated_pt.lon])
        arguments = tuple([interpolated_pt_data, lats, lons, velos])
        super().__init__(str(index) + ' ' + str(timestamp), timestamp, InterpolatedPoint, arguments)


if __name__ == '__main__':

    ap = argParser()
    ap.add_argument('filepath', type=Path, help='path to gpx file')
    args = vars(ap.parse_args())

    station_dict = StationDict()
    gpx_file = GpxFile(args['filepath'])
    route = Route(station_dict.dict, gpx_file.tree)

    empty_waypoint = route.waypoints[0]
    empty_waypoint.type = 'P'
    empty_waypoint.symbol = Waypoint.code_symbols[empty_waypoint.type]

    station_dict.add_waypoint(empty_waypoint)
    empty_waypoint.write_gpx()

    lat_values = [wp.lat for wp in route.waypoints[1:]]
    lon_values = [wp.lon for wp in route.waypoints[1:]]
    waypoint_info = tuple([empty_waypoint.name, empty_waypoint.lat, empty_waypoint.lon])

    if route.waypoints[0].velocity_csv_path.exists():
        velocities_frame = read_df(route.waypoints[0].velocity_csv_path)
    else:
        velocity_frames = [read_df(wp.velocity_csv_path).rename(columns={'Velocity_Major': 'VM' + str(i)}) for i, wp in enumerate(route.waypoints[1:])]
        velocities_frame = reduce(lambda left, right: pd.merge(left, right, on=['stamp', 'Time']), velocity_frames)
        velocities_frame.insert(loc=2, column='Velocity_Major', value=np.nan)
        print_file_exists(write_df(velocities_frame, route.waypoints[0].velocity_csv_path))
        del velocity_frames

    bite = 1000
    bites = ceil(velocities_frame.Velocity_Major.isna().sum() / bite)

    job_manager = JobManager()

    for count in range(bites):
        start = velocities_frame[velocities_frame.Velocity_Major.isna()].index[0]
        end = start + bite if start + bite < len(velocities_frame) else len(velocities_frame)
        print(start, end)

        keys = []
        for i, stamp in enumerate(velocities_frame[start:end].stamp):
            velocities = velocities_frame.iloc[i + start, 3:].values.flatten().tolist()
            key = job_manager.submit_job(InterpolatePointJob(empty_waypoint, lat_values, lon_values, velocities, stamp, i + start))
            keys.append(key)
        job_manager.wait()

        for key in keys:
            row_index = velocities_frame.index.get_loc(velocities_frame[velocities_frame['stamp'] == key].index[0])
            velocities_frame.loc[row_index, 'Velocity_Major'] = job_manager.get_result(key).velocity

        print_file_exists(write_df(velocities_frame, route.waypoints[0].velocity_csv_path))


    job_manager.stop_queue()
