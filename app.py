import os
from flask import Flask, flash, request, redirect, url_for, session
from werkzeug.utils import secure_filename
from flask_cors import CORS, cross_origin

import constants
import views

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'
app.config['UPLOAD_FOLDER'] = constants.UPLOAD_FOLDER
app.config['ALLOWED_EXTENSIONS'] = constants.ALLOWED_EXTENSIONS
app.secret_key = 'super secret key'
app.config['SESSION_TYPE'] = 'filesystem'

app.register_blueprint(views.app_blueprint)

if __name__ == "__main__":
    app.secret_key = os.urandom(24)
    app.run(debug=True,host="0.0.0.0",use_reloader=False)
