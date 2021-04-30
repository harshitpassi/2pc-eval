from bottle import run, route, get, post, request, template, response, debug
import json
from concurrent.futures import as_completed
from requests_futures.sessions import FuturesSession
import logging

#logging
logger = logging.getLogger('myApp')
logger.setLevel(logging.INFO)
fh = logging.FileHandler('server_2_log.log')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh) 



# Create a dictionary to serve as key-value store
data_store = {"1":"1000000000", "2":"1000000000"}

# Buffer for updates to be committed by transactions
transaction_buffer = {}
# map to store per key locks for clients
lock_map = {}

# Declaring server ID according to config
print("Enter server ID: (make sure it matches the order in the client config, line: 1 -> server ID:1)")
server_id = int(input())
logger.info("Starting my application.")

# Getting addresses of all other servers
f = open("config", "r", encoding="utf-8")
addresses = f.readlines()
del addresses[server_id-1]

# Initialize transaction IDs for this server
transaction_id = server_id * 1000000

@get('/')
def home():
    return {"result": "API Reachable"}

@get('/kv/begin_transaction')
def init():
    global transaction_buffer
    global data_store
    logger.info('Beginning transaction')
    transaction_buffer.clear()
    transaction_buffer = data_store.copy()
    return {'result': True}

@get('/kv/drop_transaction')
def drop():
    global transaction_buffer
    logger.info('Dropping transaction')
    transaction_buffer.clear()
    return {'result': True}

@get('/kv/commit_transaction')
def commit():
    global transaction_buffer
    global transaction_id
    global data_store
    logger.info('Committing transaction')
    # Initialize future session for creating asynchronous HTTP calls
    with FuturesSession(adapter_kwargs={'max_retries': 0}) as session:
        # increment transaction ID
        transaction_id = transaction_id + 1
        # PHASE 1 : Initialize list of prepare API calls, to prepare other servers for transaction
        logger.info('Preparing transaction')
        response_futures = [
            session.get(
                address.rstrip() +
                "kv/prepare/{}".format(transaction_id)) for address in addresses]
        # initializing list to store responses from server
        results = []
        # processing responses as they're received
        for future in as_completed(response_futures):
            try:
                logger.info('Prepare response received')
                logger.info(future.result().json())
                results.append(future.result().json())
            except BaseException:
                logger.info('Servers unavailable.')
                continue
        server_responses = [x['result'] for x in results]
        if False in server_responses:
            logger.info('Dropping transactions')
            response_futures = [
            session.get(
                address.rstrip() +
                "kv/drop_transaction") for address in addresses]
            for future in as_completed(response_futures):
                try:
                    logger.info('Dropped at server')
                    continue
                except BaseException:
                    continue
                    logger.info('Server unavailable')
            transaction_buffer.clear()
            return {'result': False}
        else:
            # PHASE 2 : create request payload with transaction buffer
            logger.info('******************** beginning phase 2')
            payload =  transaction_buffer
            logger.info('+++++++++++ PAYLOAD FOR FINAL COMMIT')
            logger.info(payload)
            request_futures = [
            session.post(
                address.rstrip() + "kv/final_commit",
                data=json.dumps(payload)) for address in addresses]
            for future in as_completed(response_futures):
                try:
                    logger.info('Commited at server')
                    continue
                except BaseException:
                    logger.info('Server unavailable')
                    continue
            data_store.clear()
            data_store = transaction_buffer.copy()
            transaction_buffer.clear()
            return {'result': True}

@get('/kv/prepare/<tid>')
def distributed_prepare(tid):
    logger.info('Preparing transaction for TID: {}'.format(tid))
    return {'result': True}

@post('/kv/final_commit')
def distributed_commit():
    global data_store
    logger.info('+++++++++++++ FINAL COMMIT ++++++++++++++++++++')
    data = json.loads(request.body.read().decode('utf-8'))
    logger.info(data)
    data_store.clear()
    data_store = data.copy()
    logger.info('Final data store is:')
    logger.info(data_store)
    return {'result': True}



# ********************** Read/Write protocol implementation start 

# Write protocol => if entry exists, update it, otherwise create a new
# key-value pair


@post('/kv/write')
def create_kv():
    global transaction_buffer
    data = json.loads(request.body.read().decode('utf-8'))
    logger.info(data)
    transaction_buffer[data['key']] = data['value']
    return {'result': True}

# Read protocol => If key exists, send an item, otherwise send a blank object


@get('/kv/read/<key>')
def read_kv(key):
    global transaction_buffer
    global data_store
    result = transaction_buffer.get(key, None)
    if(result is None):
        store = data_store.get(key, None)
        if(store is None):
            return {}
        else:
            return store
    return result

# Delete protocol => Used to delete existing entries
@route('/kv/del/<key>')
def delete_handler(key):
    global transaction_buffer
    transaction_buffer.pop(key)
    return 0

# Checks if lock_map contains key, if not, adds it and returns server id,
# otherwise returns false


@post('/kv/acquire_locks/')
def acquire_write_lock():
    global lock_map
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
    global lock_map
    data = json.loads(request.body.read().decode('utf-8'))
    client_id = data['id']
    key_list = data['keys']
    for k in key_list:
        if lock_map.get(k, None) is not None:
            if client_id == lock_map[k]:
                del lock_map[k]
    logger.info(lock_map)
    return {'result': True}

run(host='0.0.0.0', port=8081)
