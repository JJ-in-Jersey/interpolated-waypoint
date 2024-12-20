from argparse import ArgumentParser as argParser
from os import makedirs
from pathlib import Path
import pandas as pd
from functools import reduce

from tt_noaa_data.noaa_data import StationDict
from tt_gpx.gpx import GpxFile, Route, Waypoint
from tt_file_tools.file_tools import read_df, write_df
from tt_globals.globals import PresetGlobals

from sympy import Point
from tt_interpolation.velocity_interpolation import Interpolator as VInt
from tt_job_manager.job_manager import Job, JobManager


class InterpolatedPoint:

    def __init__(self, interpolation_pt_data, lats, lons, velos):
        num_points = range(len(velos))
        surface_points = tuple([Point(lats[i], lons[i], velos[i]) for i in num_points])
        interpolator = VInt(surface_points)
        interpolator.set_interpolation_point(Point(interpolation_pt_data[1], interpolation_pt_data[2], 0))
        self.velocity = tuple([min(velos), round(interpolator.get_interpolated_point().z.evalf(), 2), max(velos)])


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
    empty_waypoint.folder = PresetGlobals.waypoints_folder.joinpath(empty_waypoint.name)
    empty_waypoint.type = 'P'
    empty_waypoint.symbol = Waypoint.code_symbols[empty_waypoint.type]
    station_dict.add_waypoint(empty_waypoint)
    empty_waypoint.write_gpx()
    makedirs(empty_waypoint.folder, exist_ok=True)

    lat_values = [wp.lat for wp in route.waypoints[1:]]
    lon_values = [wp.lon for wp in route.waypoints[1:]]
    velocity_frames = [read_df(wp.velocity_csv_path).rename(columns={'Velocity_Major': 'VM' + str(i)}) for i, wp in enumerate(route.waypoints[1:])]
    velocities_frame = reduce(lambda left, right: pd.merge(left, right, on=['stamp', 'Time']), velocity_frames)

    job_manager = JobManager()
    keys = []
    for i, stamp in enumerate(velocities_frame.stamp):
        velos = velocities_frame.iloc[i, 2:].values.flatten().tolist()
        key = job_manager.submit_job(InterpolatePointJob(empty_waypoint, lat_values, lon_values, velos, stamp, i))
        keys.append(key)
    job_manager.wait()

