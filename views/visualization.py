import os
from flask import request, session, Blueprint, send_file
from werkzeug.utils import secure_filename

import constants
import logger

import pandas as pd


current_logger = logger.logger
UPLOAD_FOLDER = constants.UPLOAD_FOLDER
visualization_blueprint = Blueprint('visualization_blueprint',__name__,url_prefix='/visualize')

@visualization_blueprint.route('/duration_distribution/<path:upload_id>', methods=['GET'])
def get_duration_distribution_data(upload_id):
    data_file_path = os.path.join(UPLOAD_FOLDER,upload_id,'rides_data_file_cleaned.csv')
    rides_data = pd.read_csv(data_file_path)

    duration_ranges_dict = pd.cut(rides_data['duration'], 30).value_counts().sort_index().to_dict()
    duration_ranges = list(map(lambda interval : {'left': interval.left,'right':interval.right,'value':duration_ranges_dict[interval]},list(duration_ranges_dict.keys())))

    return duration_ranges

