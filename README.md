# CSE511 Spring'20 - Linearizable Key-Value Stores

## To run
1. Make sure Python 3 and pipenv are installed on your system.
2. Go into the project directory and run `pipenv install`
3. Once the dependencies have finished installing, run `pipenv shell`
4. Make sure the mongo connection string in `server.py` is pointing to a local mongo db.
5. Then you can run `python server.py` to run the server. Enter a server ID matching the order of the server inside the config.
6. If you're running a client instead, then run `python <client_file_name>.py`
7. The config file should contain the addresses of the servers, in order of the server IDs given to each server.
8. There are two clients offered: ABD and Blocking

## To test linearizability
1. There are EDN files of the executions in the EDN folder.
2. The test folder contains test scripts that can be executed to generate new EDN files.
3. Make sure you have no edn files in the `test` folder.
3. The clients can be run concurrently using the bash/zsh `&` operator. For example, if you want to concurrently run 3 blocking clients, you would have to run: `python3 blocking_client_api.py&python3 blocking_client_api2.py&python3 blocking_client_api3.py`.
4. This would generate 3 timestamped edn files, that you can combine and sort using the unix `sort` function like so: `sort -u --files0-from=<(printf '%s\0' *.edn) -o output`.
5. `output` would contain sorted and timestamped log entries from all processes. The timestamps and colons can be removed using a macro in your favorite text editor. Add a `[` in the beginning of the file and `]` at the end.
6. Add a `.edn` extension, and your file would be ready to run against the knossos framework.

## Mongo KVStore API
### Write KV Pair
**Post**: <Host>:8080/kv/write
Pass key, value as params

### Read KV Pair
**Get**: \<Host>:8080/kv/read/\<key>
URL should contain the key to be read

### Acquire per-key lock for blocking protocol
**Get**: \<Host>:8080/kv/blocking/acquire_lock/\<key>
URL should contain the key you want to acquire a lock against

### Release per-key lock for blocking protocol
**Get**: \<Host>:8080/kv/blocking/release_lock/\<key>
URL should contain the key you want to release a lock against

### Delete KV Pair
**Route**: \<Host>:8080/kv/del/\<key>
URL should contain the key to be deleted




