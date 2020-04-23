from bottle import run, route, get, post, request, template, response
from pymongo import MongoClient
import json

# Create a connection with local mongo db to serve as key-value store
# TO-DO - update entries according to servers
client = MongoClient("mongodb+srv://test:test@cluster0-cntdv.mongodb.net/test?retryWrites=true&w=majority&authSource=admin")
db = client.get_database('test')
collection = db.get_collection('store-1')

# Declaring server ID according to config
server_id = 1

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
# maps to store per key locks for clients => shared lock for reads, exclusive lock for writes
write_lock_map = {}
read_lock_map = {}

# Checks if write_lock_map and read_lock_map contains key, if not, adds it and returns true, otherwise returns false
@get('/kv/blocking/acquire_write_lock/<key>')
def acquire_write_lock(key):
    client_id = request.query.id
    if write_lock_map.get(str(key), None) == None and read_lock_map.get(str(key), None) == None :
        write_lock_map[str(key)] = client_id
        return {'result': server_id}
    else:
        return {'result': False}

# Checks if write_lock_map contains key, if not, adds it to read_lock_map and returns true, otherwise returns false
@get('/kv/blocking/acquire_read_lock/<key>')
def acquire_read_lock(key):
    client_id = request.query.id
    new_key_lock = []
    if write_lock_map.get(str(key), None) == None :
        if read_lock_map.get(str(key), None) == None :
            new_key_lock.append(client_id)
        else:
            new_key_lock = read_lock_map.get(str(key), []).append(client_id)
        read_lock_map[str(key)] = new_key_lock
        return {'result': server_id}
    else:
        return {'result': False}

# Releases write lock by deleting entry in write_lock_map
@get('/kv/blocking/release_write_lock/<key>')
def release_write_lock(key):
    if write_lock_map.get(str(key), None) != None :
        del write_lock_map[str(key)]
        return {'result': True}
    else:
        return {'result': False}

# Releases read lock by removing array element in shared lock. If array is empty, deletes the key from lock entirely
@get('/kv/blocking/release_read_lock/<key>')
def release_read_lock(key):
    client_id = request.query.id
    if client_id in read_lock_map.get(str(key), []):
        read_lock_map[str(key)].remove(client_id)
        if len(read_lock_map[str(key)] == 0):
            del read_lock_map[str(key)]
        return {'result': True}
    else:
        return {'result': False}

# Delete protocol => Used to delete existing entries
@route('/kv/del/<key>')
def delete_handler(key):
    collection.delete_one({'key': int(key)})
    return 0

run(host='localhost', port=60000)