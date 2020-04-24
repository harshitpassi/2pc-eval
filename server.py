from bottle import run, route, get, post, request, template, response
from pymongo import MongoClient
import json

# Create a connection with local mongo db to serve as key-value store
# TO-DO - update entries according to servers
client = MongoClient("mongodb+srv://test:test@cluster0-cntdv.mongodb.net/test?retryWrites=true&w=majority&authSource=admin")
db = client.get_database('test')
collection = db.get_collection('store-1')

# Declaring server ID according to config
print("Enter server ID: (make sure it matches the order in the client config, line: 1 -> server ID:1)")
server_id = int(input())

# ********************** Read/Write protocol implementation start (Common for ABD and Blocking protocols) **********************

# Write protocol => if entry exists, update it, otherwise create a new key-value pair
@post('/kv/write')
def create_kv():
    data = json.loads(request.body.read().decode('utf-8'))
    print(data)
    # Check if key exists and insert
    if not collection.find_one({'key': data['key']}, {'_id': 0}):
        collection.insert_one({'key': data['key'], 'value': data['value'], 'ts': {'id': int(data['ts']['id']), 'integer': int(data['ts']['integer'])}})
        return 0
    else:
        collection.find_one_and_update({'key': data['key']}, {'$set': {'value': data['value'], 'ts': {'id': int(data['ts']['id']), 'integer': int(data['ts']['integer'])}}})
        return 0

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

run(host='localhost', port=60000)