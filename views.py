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

    # read rides_data
    rides_data = pd.read_csv(os.path.join(target,'rides_data_file_raw.csv'))

    # read station_data
    station_data = pd.read_csv(os.path.join(target,'station_data_file_raw.csv'))
    
    # get duplicate records of rides_data
    rides_data_duplicates = rides_data[rides_data.duplicated('rental_id')]

    # drop duplicate records which have empty values
    rides_data_duplicates = rides_data_duplicates[rides_data_duplicates.notna()]

    # only keep the first record among duplicates and discard the remaining duplicates 
    rides_data_duplicates.drop_duplicates(subset='rental_id',keep = 'first', inplace = True)

    # from original rides_data drop all duplicated records
    rides_data.drop_duplicates(subset='rental_id',keep = False, inplace = True)

    # merged the cleaned duplicates to original rides_data
    rides_data_without_duplicates = pd.concat([rides_data, rides_data_duplicates], ignore_index=True)

    # ensure type of station_id is int64
    rides_data_without_duplicates['start_station_id'] = rides_data_without_duplicates['start_station_id'].astype('int64')
    rides_data_without_duplicates['end_station_id'] = rides_data_without_duplicates['end_station_id'].astype('int64')
    
    # make sure the station id name pair is correct
    def station_detail_integrity(station_details):
        end_station_id = station_details['end_station_id']
        end_station_name = station_details['end_station_name']
        
        derived_end_station_id = station_data.loc[station_data['name'] == end_station_name,'id']
        derived_end_station_name = station_data.loc[station_data['id'] == end_station_id,'name']
        
        if len(derived_end_station_id):
            station_details['end_station_name'] = station_data.loc[station_data['id'] == derived_end_station_id.values[0],'name'].values[0]
        elif len(derived_end_station_name):
            station_details['end_station_id'] = station_data.loc[station_data['name'] == derived_end_station_name.values[0],'id'].values[0]
        else:
            station_details['end_station_name'] = 'NA'
            
        start_station_id = station_details['start_station_id']
        start_station_name = station_details['start_station_name']
        
        derived_start_station_id = station_data.loc[station_data['name'] == start_station_name,'id']
        derived_start_station_name = station_data.loc[station_data['id'] == start_station_id,'name']
        
        if len(derived_start_station_id):
            station_details['start_station_name'] = station_data.loc[station_data['id'] == derived_start_station_id.values[0],'name'].values[0]
        elif len(derived_start_station_name):
            station_details['start_station_id'] = station_data.loc[station_data['name'] == derived_start_station_name.values[0],'id'].values[0]
        else:
            station_details['start_station_name'] = 'NA'
        
        
        return station_details
    

    # applies station_detail_integrity function to every record of rides_data_without_duplicates
    rides_data_without_duplicates = rides_data_without_duplicates.apply(station_detail_integrity, 1)

    # remove values which are equal to 'NA'
    rides_data_without_duplicates = rides_data_without_duplicates[rides_data_without_duplicates['end_station_name'] != 'NA']
    rides_data_without_duplicates = rides_data_without_duplicates[rides_data_without_duplicates['start_station_name'] != 'NA']

    # remove values which are empty
    rides_data_without_duplicates = rides_data_without_duplicates[rides_data_without_duplicates['end_station_name'].notna()]
    rides_data_without_duplicates = rides_data_without_duplicates[rides_data_without_duplicates['start_station_name'].notna()]
    rides_data_without_duplicates = rides_data_without_duplicates[rides_data_without_duplicates['end_station_id'].notna()]
    rides_data_without_duplicates = rides_data_without_duplicates[rides_data_without_duplicates['end_station_id'].notna()]
    rides_data_without_duplicates = rides_data_without_duplicates[rides_data_without_duplicates['start_station_name'] != 'Adress is missing']

    # saving cleaned rides_data
    cleaned_rides_data_file_destination = "/".join([target, 'rides_data_file_cleaned.csv'])
    rides_data_without_duplicates.to_csv(cleaned_rides_data_file_destination,index=False)

    
    # drop station_data columns which are irrelavant for visualization and analysis
    station_data.drop(['installed','locked','install_date','removal_date'],axis=1,inplace=True)
    station_data.sort_values('id',inplace=True)
    # saving cleaned station_data in a file
    cleaned_station_data_file_destination = "/".join([target, 'station_data_file_cleaned.csv'])
    station_data.to_csv(cleaned_station_data_file_destination, index=False)

    return {'message':'Successfully cleaned data'}
