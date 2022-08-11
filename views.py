import os
from flask import request, session, Blueprint, send_file
from werkzeug.utils import secure_filename

import constants
import logger
import time
import uuid
from glob import glob
from io import BytesIO
from zipfile import ZipFile

import pandas as pd

current_logger = logger.logger
UPLOAD_FOLDER = constants.UPLOAD_FOLDER
app_blueprint = Blueprint('app_blueprint',__name__,url_prefix='/')

@app_blueprint.route('/upload', methods=['POST'])
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

@app_blueprint.route('/download/<path:upload_id>', methods=['GET', 'POST'])
def download(upload_id):
    target = os.path.join(os.getcwd(),constants.UPLOAD_FOLDER,upload_id)
    stream = BytesIO()
    with ZipFile(stream, 'w') as zf:
        for file in glob(os.path.join(target, '*.csv')):
            zf.write(file, os.path.basename(file))
    stream.seek(0)

    return send_file(
        stream,
        download_name='${upload_id}.zip',
        mimetype='application/zip'
    )

@app_blueprint.route('/clean_data/<path:upload_id>', methods=['GET'])
def clean_data(upload_id):

    target=os.path.join(UPLOAD_FOLDER,upload_id)

    # if not os.path.isdir(target):
    #     os.mkdir(target)
    rides_data = pd.read_csv(os.path.join(target,'rides_data_file_raw.csv'))

    cleaned_file_name = 'rides_data_file_cleaned.csv'
    cleaned_file_destination = "/".join([target, cleaned_file_name])
    rides_data.to_csv(cleaned_file_destination)
    # rides_data_file_raw.save(cleaned_file_destination)


    return {'message':'Successfully cleaned data'}
