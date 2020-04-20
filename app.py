from bottle import run, request
from utilities import get_bottle_app_with_mongo


# Get App instance
app = get_bottle_app_with_mongo("mongodb://127.0.0.1", "p2_511")


# Handle kv creation
@app.route('/kv/insert')
def create_kv(mongodb):
    collection = mongodb['kvstore']
    data = request.query
    # Check if key exists and insert
    if not collection.find_one({'key': data['key']}):
        collection.insert_one({'key': data['key'], 'value': data['value']})
        return 0
    else:
        return -1


@app.route('/kv/read/<key>')
def read_kv(mongodb, key):
    collection = mongodb['kvstore']
    return collection.find_one({'key': key}, {'_id': 0})


@app.route('/kv/up/<key>')
def update_kv(mongodb, key):
    collection = mongodb['kvstore']
    data = request.query
    if not collection.find_one({'key': key}):
        # Key-Value does not exist
        collection.insert_one({'key': key, 'value': data['value']})
    else:
        collection.find_one_and_update({'key': key}, {'$set': {'value': data['value']}})
    return 0


@app.route('/kv/del/<key>')
def delete_handler(mongodb, key):
    collection = mongodb['kvstore']
    collection.delete_one({'key': key})
    return 0


run(app, host='localhost', port=8080)
