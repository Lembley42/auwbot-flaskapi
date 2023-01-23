import json
from bson import ObjectId
from datetime import datetime

DEFAULT_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
DEFAULT_BYTE_FORMAT = 'utf-8'

class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        elif isinstance(obj, bytes):
            return obj.decode(DEFAULT_BYTE_FORMAT)
        elif isinstance(obj, datetime):
            return obj.strftime(DEFAULT_DATE_FORMAT)
        return json.JSONEncoder.default(self, obj)

