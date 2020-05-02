from bottle import run, route, get, post, request, template, response
from pymongo import MongoClient
import json

# Create a connection with local mongo db to serve as key-value store
# TO-DO - update entries according to servers
client = MongoClient()
db = client.get_database('test')
collection = db.get_collection('store-1')

# Declaring server ID according to config
print("Enter server ID: (make sure it matches the order in the client config, line: 1 -> server ID:1)")
server_id = int(input())


@get('/')
def home():
    return {"result": "API Reachable"}

# ********************** Read/Write protocol implementation start (Common for ABD and Blocking protocols) **********************

# Write protocol => if entry exists, update it, otherwise create a new key-value pair
@post('/kv/write')
def create_kv():
    data = json.loads(request.body.read().decode('utf-8'))
    print(data)
    # Check if key exists and insert
    if not collection.find_one({'key': data['key']}, {'_id': 0}):
        collection.insert_one({'key': data['key'], 'value': data['value'], 'ts': {'id': int(data['ts']['id']), 'integer': int(data['ts']['integer'])}})
        return {'result': True}
    else:
        old_ts = collection.find_one({'key': data['key']},  {'_id': 0})
        timestamp_response = old_ts.get("ts", {}).get("integer", 0)
        timestamp_client = old_ts.get("ts", {}).get("id", 0)
        if timestamp_response < data.get("ts", {}).get("integer", 0):
            collection.find_one_and_update({'key': data['key']}, {'$set': {'value': data['value'],'ts': {'id': int(data['ts']['id']),'integer': int(data['ts']['integer'])}}})
            return {'result': True}
        elif timestamp_response == data.get("ts", {}).get("integer", 0) and timestamp_client < data.get("ts", {}).get("id", 0):
            collection.find_one_and_update({'key': data['key']}, {'$set': {'value': data['value'],'ts': {'id': int(data['ts']['id']),'integer': int(data['ts']['integer'])}}})
            return {'result': True}
        elif timestamp_response == data.get("ts", {}).get("integer", 0):
            return {'result': True}
        return {'result': False}

# Read protocol => If key exists, send an item, otherwise send a blank object
@get('/kv/read/<key>')
def read_kv(key):
    result = collection.find_one({'key': key}, {'_id': 0})
    if(result == None):
        return {}
    return result

# ********************** Blocking protocol implementation start **********************
# map to store per key locks for clients
lock_map = {}

# Checks if lock_map contains key, if not, adds it and returns server id, otherwise returns false
@get('/kv/blocking/acquire_lock/<key>')
def acquire_write_lock(key):
    client_id = request.query.id
    if lock_map.get(str(key), None) == None :
        lock_map[str(key)] = client_id
        return {'result': server_id}
    else:
        return {'result': False}

# Releases lock by deleting entry in lock_map
@get('/kv/blocking/release_lock/<key>')
def release_write_lock(key):
    if lock_map.get(str(key), None) != None :
        del lock_map[str(key)]
        return {'result': True}
    else:
        return {'result': False}

# Delete protocol => Used to delete existing entries
@route('/kv/del/<key>')
def delete_handler(key):
    collection.delete_one({'key': int(key)})
    return 0

run(host='0.0.0.0', port=8080)