import os
from flask import request, session, Blueprint, send_file
from werkzeug.utils import secure_filename

import constants
import logger
import time
import uuid

current_logger = logger.logger
UPLOAD_FOLDER = constants.UPLOAD_FOLDER
app_blueprint = Blueprint('app_blueprint',__name__,url_prefix='/')

@app_blueprint.route('/upload', methods=['POST'])
def fileUpload():
    upload_id = str(uuid.uuid4())
    target=os.path.join(UPLOAD_FOLDER,upload_id)
    if not os.path.isdir(target):
        os.mkdir(target)
    current_logger.info("welcome to upload`")
    file = request.files['file'] 
    filename = secure_filename(file.filename)
    destination="/".join([target, filename])
    file.save(destination)
    cleaned_file_destination = "/".join([target, 'cleaned_file.csv'])
    file.save(cleaned_file_destination)
    session['uploadFilePath']=destination
    response={'file_upload_status':'SUCCESS','upload_id' : upload_id}
    return response

@app_blueprint.route('/download/<path:upload_id>', methods=['GET', 'POST'])
def download(upload_id):
    path = os.path.join(os.getcwd(),constants.UPLOAD_FOLDER,upload_id,'cleaned_file.csv')
    print(path)
    return send_file(path, as_attachment=True)