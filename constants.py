import os

UPLOAD_FOLDER = os.path.join(os.getcwd(),'upload_files') 
ALLOWED_EXTENSIONS = set(['csv'])

VALID_DATE_PATTERN = '\d{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[1-2]\d|3[0-1])\s(?:[0-1]\d|2[0-3]):[0-5]\d:[0-5]\d\sUTC'
