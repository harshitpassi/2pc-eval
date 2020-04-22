# CSE511 Spring'20 - Linearizable Key-Value Stores

## To run
1. Make sure Python 3 and pipenv are installed on your system.
2. Go into the project directory and run `pipenv install`
3. Once the dependencies have finished installing, run `pipenv shell`
4. Then you can run `python server.py` to run the server.
5. Client instructions coming soon.

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




