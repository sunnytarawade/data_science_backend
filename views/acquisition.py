import os
from flask import request, session, Blueprint, send_file
from werkzeug.utils import secure_filename

import constants
import logger
import uuid

current_logger = logger.logger
UPLOAD_FOLDER = constants.UPLOAD_FOLDER
acquisition_blueprint = Blueprint('acquisition_blueprint',__name__,url_prefix='/acquisition')

@acquisition_blueprint.route('/upload', methods=['POST'])
def fileUpload():
    upload_id = str(uuid.uuid4())
    target=os.path.join(UPLOAD_FOLDER,upload_id)

    if not os.path.isdir(target):
        os.mkdir(target)


    rides_data_file_raw = request.files['ridesDataFile'] 
    rides_data_file_name_raw = secure_filename('rides_data_file_raw.csv')
    destination="/".join([target, rides_data_file_name_raw])
    rides_data_file_raw.save(destination)

    station_data_file_raw = request.files['stationDataFile'] 
    station_data_file_name_raw = secure_filename('station_data_file_raw.csv')
    destination="/".join([target, station_data_file_name_raw])
    station_data_file_raw.save(destination)

    session['uploadFilePath']=destination
    response={'file_upload_status':'SUCCESS','upload_id' : upload_id}
    return response

