from bottle import run, route, get, post, request, template, response
import json

# Create a dictionary to serve as key-value store
data_store = {}

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
    data_store[data['key']] = data['value']
    return {'result': True}

# Read protocol => If key exists, send an item, otherwise send a blank object
@get('/kv/read/<key>')
def read_kv(key):
    result = data_store.get(key, None)
    if(result == None):
        return {}
    return result

# map to store per key locks for clients
lock_map = {}

# Checks if lock_map contains key, if not, adds it and returns server id, otherwise returns false
@post('/kv/blocking/acquire_lock/')
def acquire_write_lock():
    data = json.loads(request.body.read().decode('utf-8'))
    client_id = data['id']
    key_list = data['keys']
    lock_flag = False
    for k in key_list:
        if lock_map.get(k, None) != None :
            lock_flag = False
            return {'result': False}
        else:
            lock_flag = True
    if lock_flag:
        for k in key_list:
            lock_map[k] = client_id
        return {'result': server_id}

# Releases lock by deleting entry in lock_map
@get('/kv/blocking/release_lock/')
def release_write_lock():
    data = json.loads(request.body.read().decode('utf-8'))
    key_list = data['keys']
    for k in key_list:    
        if lock_map.get(k, None) != None :
            del lock_map[k]
    return {'result': True}

# Delete protocol => Used to delete existing entries
@route('/kv/del/<key>')
def delete_handler(key):
    data_store.pop(key)
    return 0

run(host='0.0.0.0', port=8080)