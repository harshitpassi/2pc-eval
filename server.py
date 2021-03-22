from bottle import run, route, get, post, request, template, response
import json

# Create a dictionary to serve as key-value store
data_store = {}
# Buffer for updates to be committed by transactions
transaction_buffer = {}
# map to store per key locks for clients
lock_map = {}

# Declaring server ID according to config
print("Enter server ID: (make sure it matches the order in the client config, line: 1 -> server ID:1)")
server_id = int(input())


@get('/')
def home():
    return {"result": "API Reachable"}

@get('/kv/begin_transaction')
def init():
    transaction_buffer.clear()
    transaction_buffer = data_store.copy()
    return {'result': True}

@get('/kv/drop_transaction')
def drop():
    transaction_buffer.clear()
    return {'result': True}

@get('/kv/commit_transaction')
def commit():
    data_store.clear()
    data_store = transaction_buffer.copy()
    transaction_buffer.clear()
    # 2PC implementation right here
    return {'result': True}

# ********************** Read/Write protocol implementation start 

# Write protocol => if entry exists, update it, otherwise create a new
# key-value pair


@post('/kv/write')
def create_kv():
    data = json.loads(request.body.read().decode('utf-8'))
    print(data)
    transaction_buffer[data['key']] = data['value']
    return {'result': True}

# Read protocol => If key exists, send an item, otherwise send a blank object


@get('/kv/read/<key>')
def read_kv(key):
    result = transaction_buffer.get(key, None)
    if(result is None):
        return {}
    return result

# Delete protocol => Used to delete existing entries


@route('/kv/del/<key>')
def delete_handler(key):
    transaction_buffer.pop(key)
    return 0

# Checks if lock_map contains key, if not, adds it and returns server id,
# otherwise returns false


@post('/kv/acquire_locks/')
def acquire_write_lock():
    data = json.loads(request.body.read().decode('utf-8'))
    client_id = data['id']
    key_list = data['keys']
    lock_flag = False
    for k in key_list:
        if lock_map.get(k, None) is not None:
            lock_flag = False
            return {'result': False}
        else:
            lock_flag = True
    if lock_flag:
        for k in key_list:
            lock_map[k] = client_id
        return {'result': server_id}

# Releases lock by deleting entry in lock_map


@post('/kv/release_locks/')
def release_write_lock():
    data = json.loads(request.body.read().decode('utf-8'))
    key_list = data['keys']
    for k in key_list:
        if lock_map.get(k, None) is not None:
            del lock_map[k]
    print(lock_map)
    return {'result': True}


run(host='0.0.0.0', port=8080)
