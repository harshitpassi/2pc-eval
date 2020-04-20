# CSE511 Spring'20 - Linearizable Key-Value Stores

## Mongo KVStore API
### Insert KV Pair
**Route**: <Host>:8080/kv/insert
Pass key, value as params

### Read KV Pair
**Route**: \<Host>:8080/kv/read/\<key>
URL should contain the key to be read

### Update KV Pair
**Route**: \<Host>:8080/kv/up/\<key>
URL should contain the key to be read, and value in params

### Delete KV Pair
**Route**: \<Host>:8080/kv/del/\<key>
URL should contain the key to be deleted




