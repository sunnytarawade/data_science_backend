import os
from flask import Blueprint, send_file

import constants

from glob import glob
from io import BytesIO
from zipfile import ZipFile

import pandas as pd
import re
import json
    

UPLOAD_FOLDER = constants.UPLOAD_FOLDER
cleaning_blueprint = Blueprint('cleaning_blueprint',__name__,url_prefix='/clean')

@cleaning_blueprint.route('/download/<path:upload_id>', methods=['GET', 'POST'])
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

@cleaning_blueprint.route('/<path:upload_id>', methods=['GET'])
def clean_data(upload_id):

    target = os.path.join(UPLOAD_FOLDER,upload_id)
    data_cleaning_details = {}
    # read station_data
    station_data = pd.read_csv(os.path.join(target,'station_data_file_raw.csv'))
    
    data_cleaning_details['station_data_empty_values'] = station_data.isna().sum().to_dict()
    data_cleaning_details['station_data_columns_before_cleaning'] = station_data.columns.to_list()
    # drop station_data columns which are irrelavant for visualization and analysis
    station_data.drop(['installed','locked','install_date','removal_date'],axis=1,inplace=True)

    data_cleaning_details['station_data_columns_after_cleaning'] = station_data.columns.to_list()

    station_data.sort_values('id',inplace=True)
    # saving cleaned station_data in a file
    cleaned_station_data_file_destination = "/".join([target, 'station_data_file_cleaned.csv'])
    station_data.to_csv(cleaned_station_data_file_destination, index=False)


    # read rides_data
    rides_data = pd.read_csv(os.path.join(target,'rides_data_file_raw.csv'))

    data_cleaning_details['rides_data_total_raw_records'] = len(rides_data)
    data_cleaning_details['rides_data_empty_records_before_cleaning'] = rides_data.isna().sum().to_dict()

    # get duplicate records of rides_data
    rides_data_duplicates = rides_data[rides_data.duplicated('rental_id')]

    data_cleaning_details['rides_data_total_duplicates'] = len(rides_data_duplicates)
    # drop duplicate records which have empty values
    rides_data_duplicates = rides_data_duplicates[rides_data_duplicates.notna()]

    # only keep the first record among duplicates and discard the remaining duplicates 
    rides_data_duplicates.drop_duplicates(subset='rental_id',keep = 'first', inplace = True)
    data_cleaning_details['rides_data_total_duplicates_removed'] = data_cleaning_details['rides_data_total_duplicates'] - len(rides_data_duplicates)
    
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
        
        is_end_station_id_valid = bool(len(derived_end_station_name))
        is_end_station_name_valid = bool(len(derived_end_station_id))

        if is_end_station_id_valid:
            station_details['end_station_name'] = station_data.loc[station_data['id'] == end_station_id,'name'].values[0]
        elif is_end_station_name_valid:
            station_details['end_station_id'] = station_data.loc[station_data['name'] == end_station_name,'id'].values[0]
        else:
            station_details['end_station_id'] = 'NA'
            station_details['end_station_name'] = 'NA'
            
        start_station_id = station_details['start_station_id']
        start_station_name = station_details['start_station_name']
        
        derived_start_station_id = station_data.loc[station_data['name'] == start_station_name,'id']
        derived_start_station_name = station_data.loc[station_data['id'] == start_station_id,'name']
        
        is_start_station_id_valid = bool(len(derived_start_station_name))
        is_start_station_name_valid = bool(len(derived_start_station_id))
        
        if is_start_station_id_valid:
            station_details['start_station_name'] = station_data.loc[station_data['id'] == start_station_id,'name'].values[0]
        elif is_start_station_name_valid:
            station_details['start_station_id'] = station_data.loc[station_data['name'] == start_station_name,'id'].values[0]
        else:
            station_details['start_station_id'] = 'NA'
            station_details['start_station_name'] = 'NA'
        
        return station_details


    # applies station_detail_integrity function to every record of rides_data_without_duplicates
    rides_data_without_duplicates = rides_data_without_duplicates.apply(station_detail_integrity, 1)

    data_cleaning_details['rides_data_total_records_before_removing_incorrect_station_details'] = len(rides_data_without_duplicates)

    # remove values which are equal to 'NA'
    rides_data_without_duplicates = rides_data_without_duplicates[rides_data_without_duplicates['end_station_name'] != 'NA']
    rides_data_without_duplicates = rides_data_without_duplicates[rides_data_without_duplicates['start_station_name'] != 'NA']

    data_cleaning_details['rides_data_total_records_with_incorrect_station_details'] = data_cleaning_details['rides_data_total_records_before_removing_incorrect_station_details'] - len(rides_data_without_duplicates)

    #  ensures that duration is numeric
    def ensure_duration_is_numeric(ride_details):
        return ride_details if ride_details['duration'].isnumeric() else 'NA'

    # applies ensure_duration_is_numeric function to every record
    rides_data_without_duplicates = rides_data_without_duplicates.apply(ensure_duration_is_numeric,1)

    data_cleaning_details['rides_data_total_records_before_removing_non_numeric_duration'] = len(rides_data_without_duplicates)
    # removes records where duration is 'NA'
    rides_data_without_duplicates = rides_data_without_duplicates[rides_data_without_duplicates['duration'] != 'NA']

    data_cleaning_details['rides_data_total_records_with_non_numeric_duration'] = data_cleaning_details['rides_data_total_records_before_removing_non_numeric_duration'] - len(rides_data_without_duplicates)

    # convert duration to float type
    rides_data_without_duplicates['duration'] = rides_data_without_duplicates['duration'].astype('float')

    # handle outliers
    duration_75_percentile_value =  rides_data_without_duplicates['duration'].describe()['75%']
    rides_data_without_duplicates.loc[rides_data_without_duplicates['duration'] > duration_75_percentile_value,'duration'] = duration_75_percentile_value 

    # ensures that start_date and end_date match valid date pattern
    def ensure_date_validity(ride_details):
        are_dates_valid = bool(re.match(constants.VALID_DATE_PATTERN,ride_details['start_date'])) and bool(re.match(constants.VALID_DATE_PATTERN,ride_details['end_date']))    
        return ride_details if are_dates_valid else 'NA'

    data_cleaning_details['rides_data_total_records_before_removing_incorrect_dates'] = len(rides_data_without_duplicates)
    # applies ensure_date_validity function to all records
    rides_data_without_duplicates = rides_data_without_duplicates.apply(ensure_date_validity,1)

    # remove records where value of start_date and end_date is 'NA'
    rides_data_without_duplicates = rides_data_without_duplicates[rides_data_without_duplicates['start_date'] != 'NA']
    rides_data_without_duplicates = rides_data_without_duplicates[rides_data_without_duplicates['end_date'] != 'NA']

    data_cleaning_details['rides_data_total_records_with_incorrect_dates'] = data_cleaning_details['rides_data_total_records_before_removing_incorrect_dates'] - len(rides_data_without_duplicates)
    

    # remove values which are empty
    rides_data_without_duplicates = rides_data_without_duplicates[rides_data_without_duplicates['end_station_name'].notna()]
    rides_data_without_duplicates = rides_data_without_duplicates[rides_data_without_duplicates['start_station_name'].notna()]
    rides_data_without_duplicates = rides_data_without_duplicates[rides_data_without_duplicates['end_station_id'].notna()]
    rides_data_without_duplicates = rides_data_without_duplicates[rides_data_without_duplicates['end_station_id'].notna()]
    rides_data_without_duplicates = rides_data_without_duplicates[rides_data_without_duplicates['start_station_name'] != 'Adress is missing']

    data_cleaning_details['rides_data_total_records_after_cleaning'] = len(rides_data_without_duplicates)
    data_cleaning_details['rides_data_total_records_dropped'] = data_cleaning_details['rides_data_total_raw_records'] - data_cleaning_details['rides_data_total_records_after_cleaning']
    data_cleaning_details['rides_data_total_records_dropped_percentage'] = (data_cleaning_details['rides_data_total_records_dropped'] / data_cleaning_details['rides_data_total_raw_records']) * 100
    
    # saving cleaned rides_data
    cleaned_rides_data_file_path = "/".join([target, 'rides_data_file_cleaned.csv'])
    rides_data_without_duplicates.to_csv(cleaned_rides_data_file_path,index=False)

    data_cleaning_details_file_path = "/".join([target, 'data_cleaning_details.json'])
    with open(data_cleaning_details_file_path, 'w+') as outfile:
        json.dump(data_cleaning_details, outfile)
    
    return {'message':'Successfully cleaned data'}

@cleaning_blueprint.route('/data_cleaning_details/<path:upload_id>/', methods=['GET', 'POST'])
def get_data_cleaning_details(upload_id):
    
    target = os.path.join(UPLOAD_FOLDER,upload_id)
    
    data_cleaning_details_file_path = "/".join([target, 'data_cleaning_details.json'])
    print(data_cleaning_details_file_path)
    with open(data_cleaning_details_file_path, 'r+') as infile:
        data_cleaning_details = json.load(infile)

    return data_cleaning_details
    
