from bottle import run, route, get, post, request, template, response
from pymongo import MongoClient

client = MongoClient("mongodb+srv://test:test@cluster0-cntdv.mongodb.net/test?retryWrites=true&w=majority&authSource=admin")
db = client.get_database('test')
collection = db.get_collection('store-1')

# Handle kv creation
@post('/kv/write')
def create_kv():
    data = request.query
    # Check if key exists and insert
    if not collection.find_one({'key': int(data['key'])}, {'_id': 0}):
        collection.insert_one({'key': int(data['key']), 'value': data['value'], 'ts': int(data['ts'])})
        return 0
    else:
        collection.find_one_and_update({'key': int(data['key'])}, {'$set': {'value': data['value'], 'ts': int(data['ts'])}})
        return 0


@get('/kv/read/<key>')
def read_kv(key):
    result = collection.find_one({'key': int(key)}, {'_id': 0})
    if(result == None):
        return {}
    return result

@route('/kv/del/<key>')
def delete_handler(key):
    collection.delete_one({'key': key})
    return 0

run(host='localhost', port=60000)