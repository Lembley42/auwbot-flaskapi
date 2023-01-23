### IMPORTS ###
from flask import Flask, request, jsonify, Blueprint
import pymongo, json, os, croniter
from bson.objectid import ObjectId
from datetime import datetime, timedelta
from google.cloud import pubsub_v1
# Local Imports
from jsonencoder import JSONEncoder
from filedecryption import Decrypt_File

# Create Flask app
app = Flask(__name__)

# Get environment variables
MONGO_PATH = os.environ.get('MONGO_PATH')
MONGO_USER = os.environ.get('MONGO_USER')
MONGO_PASS = os.environ.get('MONGO_PASS')
API_KEY = os.environ.get('API_KEY')
GOOGLE_PROJECT_ID = os.environ.get('GOOGLE_PROJECT_ID')

# Set Google credentials
Decrypt_File('google-credentials.bin', 'google-credentials.json', os.environ.get('GOOGLE_CREDENTIALS_KEY'))
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'google-credentials.json'

# Connect to MongoDB
client = pymongo.MongoClient(f"mongodb+srv://{MONGO_USER}:{MONGO_PASS}@{MONGO_PATH}/?retryWrites=true&w=majority")
# Get databases
task_db = client['tasks']
googleads_db = client['googleads']
facebook_db = client['facebook']
linkedin_db = client['linkedin']

# Connect to PubSub
cloud_publisher = pubsub_v1.PublisherClient()

# Register blueprint
blueprints = [
    Blueprint('create_tasks', __name__, '/tasks/<customer_name>'),
    Blueprint('readupdatedelete_tasks', __name__, '/tasks/<id>/<customer_name>'), # Read, Update, Delete share same URL
    Blueprint('readall_tasks', __name__, '/tasks/scheduled/<type>'),
    Blueprint('log_tasks', __name__, '/tasks/log/<id>/<customer_name>'),
    Blueprint('get_date_range', __name__, '/tasks/daterange/<id>/<customer_name>'),
    Blueprint('reschedule_tasks', __name__, '/tasks/reschedule/<id>/<customer_name>'),
    Blueprint('block_tasks', __name__, '/tasks/block/<id>/<customer_name>'),
    Blueprint('unblock_tasks', __name__, '/tasks/unblock/<id>/<customer_name>'),
    Blueprint('create_googleads', __name__, '/googleads/<customer_name>'),
    Blueprint('create_facebook', __name__, '/facebook/<customer_name>'),
    Blueprint('create_linkedin', __name__, '/linkedin/<customer_name>'),
    Blueprint('send_pubsub_message', __name__, '/pubsub/<topic>')
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
        # JSON data to dict
        data = json.loads(data)
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

# Read all
@app.route('/tasks/scheduled/<type>', methods=['GET'])
def get_tasks_of_type(type):
    if request.method == 'GET':
        # Get every collection in database
        collections = task_db.list_collection_names()
        # Find every document where status is idle and where type is equal to type
        now = datetime.utcnow()
        tasks = []
        for collection in collections:
            tasks += list(task_db[collection].find({'status': 'idle', 'schedule.next_run': {'$gte': now}, 'type': type}))        
        # Convert with custom JSONEncoder to JSON
        json_data = json.dumps(tasks, cls=JSONEncoder)
        # Return tasks
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


# Get Date Range
@app.route('/tasks/daterange/<id>/<customer_name>', methods=['GET'])
def get_date_range(id, customer_name):
    if request.method == 'GET':
        # Get customer database
        collection = task_db[customer_name]
        # Get task from document by ObjectId
        task = collection.find_one({'_id': ObjectId(id)})
        # Determine date range
        mode = task['mode']
        task_settings = task['settings']
        daysToLoad = task_settings['days_per_load']
        daysToUpdate = task_settings['days_per_update']
        first_date = datetime.strptime(task_settings['first_date'], '%Y-%m-%d')
        last_date = datetime.strptime(task_settings['last_date'], '%Y-%m-%d')
        today = datetime.now()

        # If last date is within update range, ensure mode is update
        if mode == 'load' and (last_date + timedelta(days=daysToLoad)) >= today:
            mode = 'update'
            collection.update_one({'_id': ObjectId(id)}, {'$set': {'mode': mode}})
        # If last date is not within update range, ensure mode is load
        elif mode == 'update' and (last_date + timedelta(days=daysToLoad)) < today: 
            mode = 'load'
            collection.update_one({'_id': ObjectId(id)}, {'$set': {'mode': mode}})

        # When Update, start date is today and end date is today - daysToUpdate, but no earlier than first_date
        if mode == 'update':
            end_date = today
            start_date = end_date - timedelta(days=daysToUpdate)
            if end_date < first_date: end_date = first_date
        
        # When Load, start date is last_date and end date is last_date + daysToLoad, but no later than today
        elif mode == 'load':
            start_date = last_date
            end_date = start_date + timedelta(days=daysToLoad)
            if end_date > today: end_date = today
        
        # Convert to string with format #YYYY-MM-DD
        start_date = start_date.strftime('%Y-%m-%d')
        end_date = end_date.strftime('%Y-%m-%d')

        # Return date range
        return jsonify({'start_date': start_date, 'end_date': end_date})


# Reschedule
@app.route('/tasks/reschedule/<id>/<customer_name>', methods=['PUT'])
def reschedule_task(id, customer_name):
    if request.method == 'PUT':
        # Get customer database
        collection = task_db[customer_name]
        # Load task from database
        task = collection.find_one({'_id': ObjectId(id)})
        # Get variables from task document
        mode = task['mode']
        next_run = task['schedule']['next_run']
        cron_string = task['schedule'][f'cron_{mode}']
        # Increase next_run by cron schedule
        next_run = croniter.croniter(cron_string, next_run).get_next(datetime)
        # Update task
        collection.update_one({'_id': ObjectId(id)}, {'$set': {'schedule.next_run': next_run}})
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


### LINKEDIN API ###
# Create
@app.route('/linkedin/<customer_name>', methods=['POST'])
def create_linkedin(customer_name):
    if request.method == 'POST':
        # Get data from JSON response
        data = request.get_json()
        # Change data['date'] to datetime object
        data['date'] = datetime.strptime(data['date'], '%Y-%m-%d')
        # Get customer database
        collection = linkedin_db[customer_name]
        # Insert data into database if doesn't exist, else update existing data
        collection.find_one_and_update({'date': data['date'], 'id': data['id']}, {'$set': {'cost': data['cost'], 'status': data['status']}}, upsert=True)
        # Return success message
        return jsonify({'status': 'success'})


### GOOGLE PUBSUB ###
# Send
@app.route('/pubsub/<topic>', methods=['POST'])
def send_pubsub_message(topic):
    if request.method == 'POST':
        # Get data from JSON response
        data = request.get_json()
        # Get Message
        message = data['message']
        # Get topic
        topic_path = cloud_publisher.topic_path(GOOGLE_PROJECT_ID, topic)
        # Send message
        future = cloud_publisher.publish(topic_path, data=message.encode('utf-8'))
        # Return success message
        return jsonify({'status': 'success'})



if __name__ == '__main__':
    app.run(debug=True)
