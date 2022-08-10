import os
from flask import request, session, Blueprint, send_file
from werkzeug.utils import secure_filename

import constants
import logger
import time

current_logger = logger.logger
UPLOAD_FOLDER = constants.UPLOAD_FOLDER
app_blueprint = Blueprint('app_blueprint',__name__,url_prefix='/')

@app_blueprint.route('/upload', methods=['POST'])
def fileUpload():
    target=os.path.join(UPLOAD_FOLDER,'test_docs')
    if not os.path.isdir(target):
        os.mkdir(target)
    current_logger.info("welcome to upload`")
    file = request.files['file'] 
    filename = secure_filename(file.filename)
    destination="/".join([target, filename])
    file.save(destination)
    session['uploadFilePath']=destination
    response="This is response"
    return response

@app_blueprint.route('/download/<path:filename>', methods=['GET', 'POST'])
def download(filename):
    path = os.path.join(os.getcwd(),constants.UPLOAD_FOLDER,'test_docs',filename)
    return send_file(path, as_attachment=True)