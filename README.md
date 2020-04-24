# CSE511 Spring'20 - Linearizable Key-Value Stores

## To run
1. Make sure Python 3 and pipenv are installed on your system.
2. Go into the project directory and run `pipenv install`
3. Once the dependencies have finished installing, run `pipenv shell`
4. Make sure the mongo connection string in `server.py` is pointing to a local mongo db.
5. Then you can run `python server.py` to run the server. Enter a server ID matching the order of the server inside the config.
6. If you're running a client instead, then run `python <client_file_name>.py`
7. There are two clients offered: ABD and Blocking

## Mongo KVStore API
### Write KV Pair
**Post**: <Host>:8080/kv/write
Pass key, value as params

### Read KV Pair
**Get**: \<Host>:8080/kv/read/\<key>
URL should contain the key to be read

### Delete KV Pair
**Route**: \<Host>:8080/kv/del/\<key>
URL should contain the key to be deleted




