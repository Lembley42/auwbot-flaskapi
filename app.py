### IMPORTS ###
from flask import Flask, request, jsonify, Blueprint
import pymongo, json, os
from bson.objectid import ObjectId
from jsonencoder import JSONEncoder

# Create Flask app
app = Flask(__name__)

# Get environment variables
MONGO_PATH = os.environ.get('MONGO_PATH')
MONGO_USER = os.environ.get('MONGO_USER')
MONGO_PASS = os.environ.get('MONGO_PASS')
API_KEY = os.environ.get('API_KEY')


# Connect to MongoDB
client = pymongo.MongoClient(f"mongodb+srv://{MONGO_USER}:{MONGO_PASS}@{MONGO_PATH}/?retryWrites=true&w=majority")
# Get databases
task_db = client['tasks']
googleads_db = client['googleads']
facebook_db = client['facebook']
linkedin_db = client['linkedin']


# Register blueprint
blueprints = [
    Blueprint('create_tasks', __name__, '/tasks/<customer_name>'),
    Blueprint('readupdatedelete_tasks', __name__, '/tasks/<id>/<customer_name>'),
    Blueprint('log_tasks', __name__, '/tasks/log/<id>/<customer_name>'),
    Blueprint('block_tasks', __name__, '/tasks/block/<id>/<customer_name>'),
    Blueprint('unblock_tasks', __name__, '/tasks/unblock/<id>/<customer_name>'),
    Blueprint('create_googleads', __name__, '/googleads/<customer_name>'),
    Blueprint('create_facebook', __name__, '/facebook/<customer_name>'),
]

for blueprint in blueprints:
    app.register_blueprint(blueprint)


### BEFORE REQUEST ###
@app.before_request
def check_api_key():
    if request.args.get('api_key') != API_KEY:
        return jsonify({'status': 'error', 'message': 'Invalid API key'}), 403
    

### TASKS ###
# Create
@app.route('/tasks/<customer_name>', methods=['POST'])
def create_task(customer_name):
    if request.method == 'POST':
        # Get data from JSON response
        data = request.get_json()
        # Get customer database
        collection = task_db[customer_name]
        # Insert data into database
        collection.insert_one(data)
        # Return success message
        return jsonify({'status': 'success'})

# Read
@app.route('/tasks/<id>/<customer_name>', methods=['GET'])
def get_task(id, customer_name):
    if request.method == 'GET':
        # Get customer database
        collection = task_db[customer_name]
        # Get task from document by ObjectId
        task = collection.find_one({'_id': ObjectId(id)})
        # Convertwith custom JSONEncoder to JSON
        json_data = json.dumps(task, cls=JSONEncoder)
        # Return task
        return json_data

# Update
@app.route('/tasks/<id>/<customer_name>', methods=['PUT'])
def update_task(id, customer_name):
    if request.method == 'PUT':
        # Get data from JSON response
        data = request.get_json()
        # Get customer database
        collection = task_db[customer_name]
        # Update task
        collection.update_one({'_id': ObjectId(id)}, {'$set': data})
        # Return success message
        return jsonify({'status': 'success'})
        
# Delete
@app.route('/tasks/<id>/<customer_name>', methods=['DELETE'])
def delete_task(id, customer_name):
    if request.method == 'DELETE':
        # Get customer database
        collection = task_db[customer_name]
        # Delete task
        result = collection.delete_one({'_id': ObjectId(id)})
        # Return success message
        return jsonify({'status': 'success'})




### Task Functions ###
# Log
@app.route('/tasks/log/<id>/<customer_name>', methods=['PUT'])
def log_task(id, customer_name):
    if request.method == 'PUT':
        # Get data from JSON response
        data = request.get_json()
        # Get customer database
        collection = task_db[customer_name]
        # Add object to log array in task
        collection.update_one({'_id': ObjectId(id)}, {'$push': {'log': data}})
        # Return success message
        return jsonify({'status': 'success'}) 

# Block
@app.route('/tasks/block/<id>/<customer_name>', methods=['PUT'])
def block_task(id, customer_name):
    if request.method == 'PUT':
        # Get customer database
        collection = task_db[customer_name]
        # Add object to block array in task
        collection.update_one({'_id': ObjectId(id)}, {'$set': {'status': 'running'}})
        # Return success message
        return jsonify({'status': 'success'})

# Unblock
@app.route('/tasks/unblock/<id>/<customer_name>', methods=['PUT'])
def unblock_task(id, customer_name):
    if request.method == 'PUT':
        # Get customer database
        collection = task_db[customer_name]
        # Add object to block array in task
        collection.update_one({'_id': ObjectId(id)}, {'$set': {'status': 'idle'}})
        # Return success message
        return jsonify({'status': 'success'})




### Google Ads API ###
# Create
@app.route('/googleads/<customer_name>', methods=['POST'])
def create_googleads(customer_name):
    if request.method == 'POST':
        # Get data from JSON response
        data = request.get_json()
        # Get customer database
        collection = googleads_db[customer_name]
        # Insert data into database if doesn't exist, else update existing data
        collection.find_one_and_update({'date': data['date'], 'id': data['id']}, {'$set': {'cost': data['cost']}}, upsert=True)
        # Return success message
        return jsonify({'status': 'success'})




### Facebook API ###
# Create
@app.route('/facebook/<customer_name>', methods=['POST'])
def create_facebook(customer_name):
    if request.method == 'POST':
        # Get data from JSON response
        data = request.get_json()
        # Get customer database
        collection = facebook_db[customer_name]
        # Insert data into database if doesn't exist, else update existing data
        collection.find_one_and_update({'date': data['date'], 'id': data['id']}, {'$set': {'cost': data['cost'], 'status': data['status']}}, upsert=True)
        # Return success message
        return jsonify({'status': 'success'})


if __name__ == '__main__':
    app.run(debug=True)
