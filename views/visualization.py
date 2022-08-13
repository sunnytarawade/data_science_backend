import os
from flask import request, session, Blueprint, send_file
from werkzeug.utils import secure_filename

import constants
import logger

import pandas as pd


current_logger = logger.logger
UPLOAD_FOLDER = constants.UPLOAD_FOLDER
visualization_blueprint = Blueprint('visualization_blueprint',__name__,url_prefix='/visualize')

@visualization_blueprint.route('/duration_distribution/<path:upload_id>/', methods=['GET'])
def get_duration_distribution_data(upload_id):
    data_file_path = os.path.join(UPLOAD_FOLDER,upload_id,'rides_data_file_cleaned.csv')
    rides_data = pd.read_csv(data_file_path)

    duration_ranges_dict = pd.cut(rides_data['duration'], 30).value_counts().sort_index().to_dict()
    duration_ranges = list(map(lambda interval : {'left': interval.left,'right':interval.right,'value':duration_ranges_dict[interval]},list(duration_ranges_dict.keys())))

    return duration_ranges

@visualization_blueprint.route('/popular_stations/<path:upload_id>/', methods=['GET'])
def get_popular_station_map_data(upload_id):
    rides_data_file_path = os.path.join(UPLOAD_FOLDER,upload_id,'rides_data_file_cleaned.csv')
    rides_data = pd.read_csv(rides_data_file_path)

    station_data_file_path = os.path.join(UPLOAD_FOLDER,upload_id,'station_data_file_cleaned.csv')
    station_data = pd.read_csv(station_data_file_path)


    start_station_freq_dict = rides_data['start_station_id'].sort_values().value_counts().to_dict()
    end_station_freq_dict = rides_data['end_station_id'].sort_values().value_counts().to_dict()

    station_frequency_dict = {int(k): start_station_freq_dict.get(k, 0) + end_station_freq_dict.get(k, 0) for k in set(start_station_freq_dict) | set(end_station_freq_dict)}

    sorted_station_frequency_dict = {k: v for k, v in sorted(station_frequency_dict.items(), key=lambda item: -item[1])}
    most_popular_stations_ids = list(sorted_station_frequency_dict.keys())[:20]

    def add_station_access_count(station_details):
            station_details['access_count'] = station_frequency_dict[station_details['id']]
            return station_details

    most_popular_stations = station_data[station_data['id'].isin(most_popular_stations_ids)]
    most_popular_stations = most_popular_stations.apply(add_station_access_count,1)
    most_popular_stations = most_popular_stations.sort_values('access_count', ascending=False)
    most_popular_stations_dict = most_popular_stations[['id','name','latitude','longitude','access_count']].to_dict('records')

    return most_popular_stations_dict
