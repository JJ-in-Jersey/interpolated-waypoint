from argparse import ArgumentParser as argParser
from pathlib import Path
from pandas import merge
from numpy import nan

from functools import reduce
from math import ceil

from tt_dataframe.dataframe import DataFrame
from tt_noaa_data.noaa_data import StationDict
from tt_gpx.gpx import GpxFile, Route, Waypoint
from tt_file_tools.file_tools import print_file_exists

from tt_job_manager.job_manager import Job, JobManager
from tt_jobs.jobs import InterpolatedPoint, InterpolatePointJob

if __name__ == '__main__':

    ap = argParser()
    ap.add_argument('filepath', type=Path, help='path to gpx file')
    args = vars(ap.parse_args())

    station_dict = StationDict()
    gpx_file = GpxFile(args['filepath'])
    route = Route(station_dict, gpx_file.tree)

    empty_waypoint = route.waypoints[0]
    empty_waypoint.type = 'P'
    empty_waypoint.symbol = Waypoint.code_symbols[empty_waypoint.type]

    station_dict.add_waypoint(empty_waypoint)
    empty_waypoint.write_gpx()

    lat_values = [wp.lat for wp in route.waypoints[1:]]
    lon_values = [wp.lon for wp in route.waypoints[1:]]
    waypoint_info = tuple([empty_waypoint.name, empty_waypoint.lat, empty_waypoint.lon])

    if route.waypoints[0].velocity_csv_path.exists():
        velocity_frame = DataFrame(csv_source=route.waypoints[0].velocity_csv_path)
    else:
        velocity_frame = (reduce(lambda left, right: merge(left, right, on=['stamp', 'Time']),
            [DataFrame(csv_source=wp.velocity_csv_path).filter(['Time', 'stamp', 'Velocity_Major']).rename(columns={'Velocity_Major': 'VM' + str(i)}) for i, wp in enumerate(route.waypoints[1:])]))
        velocity_frame.insert(loc=2, column='Velocity_Major', value=nan)
        print_file_exists(velocity_frame.write(route.waypoints[0].velocity_csv_path))

    bite = 1000
    bites = ceil(velocity_frame.Velocity_Major.isna().sum() / bite)

    job_manager = JobManager()

    for count in range(bites):
        start = velocity_frame[velocity_frame.Velocity_Major.isna()].index[0]
        end = start + bite if start + bite < len(velocity_frame) else len(velocity_frame)
        print(start, end)

        keys = []
        for i, stamp in enumerate(velocity_frame[start:end].stamp):
            velocities = velocity_frame.iloc[i + start, 3:].values.flatten().tolist()
            key = job_manager.submit_job(InterpolatePointJob(empty_waypoint, lat_values, lon_values, velocities, stamp, i + start))
            keys.append(key)
        job_manager.wait()

        for key in keys:
            row_index = velocity_frame.index.get_loc(velocity_frame[velocity_frame['stamp'] == key].index[0])
            velocity_frame.loc[row_index, 'Velocity_Major'] = job_manager.get_result(key).velocity

        print_file_exists(velocity_frame.write(route.waypoints[0].velocity_csv_path))

    job_manager.stop_queue()
