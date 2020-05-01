from concurrent.futures import as_completed
from requests_futures.sessions import FuturesSession
from utilities import retry_with_backoff
import math, random, time

# Hardcoded per client unique ID
client_id = 2

# Read the config file for a list of addresses for all the servers
f = open("config", "r", encoding="utf-8")
addresses = f.readlines()
num_servers = len(addresses)
final_count = 0

# Initialize the count of the majority quorum needed for operations
if(num_servers%2 == 0):
    final_count = num_servers/2 + 1
else:
    final_count = math.ceil(num_servers/2)


# Handler function to query a majority of servers and get the latest item, for the first phase of reads and writes 
def query_majority_servers(key, majority_addresses, session):
    responses = []
    latest_item = {}
    # Initialize list of read API calls, to get current timestamps in each server, all sent simultaneously
    response_futures = [session.get(address.rstrip() + "kv/read/{}".format(key)) for address in majority_addresses]
    # Handle the calls as they are completed
    for future in as_completed(response_futures):
        responses.append(future.result().json())
    # Get the latest item from the received responses
    for response in responses:
        timestamp_response = response.get("ts", {}).get("integer", 0)
        timestamp_client = response.get("ts", {}).get("id", 0)
        if(timestamp_response > latest_item.get("ts", {}).get("integer", 0)):
            latest_item = response
        elif(timestamp_response == latest_item.get("ts", {}).get("integer", 0) and timestamp_client > latest_item.get("ts", {}).get("id", 0)):
            latest_item = response
    return latest_item

# Handler function to release acquired locks
def release_all_locks(key, session):
    # Release locks
    unlock_futures = [session.get(address.rstrip() + "kv/blocking/release_lock/{}".format(key)) for address in addresses]
    count=0
    for future in as_completed(unlock_futures):
        count += 1
        if(count <= final_count):
            print(future.result())
        else:
            print("Majority ACKs received")
            break


def write(key, value):
    count = 0
    server_granting_locks = []
    # Initialize future session for creating asynchronous HTTP calls
    with FuturesSession() as session:

        # Acquire write locks from majority and save their server IDs in an array
        lock_futures = [session.get(address.rstrip() + "kv/blocking/acquire_lock/{}".format(key), params={'id': client_id}) for address in addresses]
        # Handle the calls as they are completed, breaking when the majority number has been reached
        for future in as_completed(lock_futures):
            count += 1
            if(count <= final_count):
                server_granting_locks.append(future.result().json())
                print(future.result().json())
            else:
                break
        server_granting_locks = [x['result'] for x in server_granting_locks]
        if False in server_granting_locks:
            print('Unable to acquire lock from majority. Please try again.')
            # Release locks
            release_all_locks(key, session)
            log_output(str(time.time()) + ' : ' +"{{:process {id}, :type :fail, :f :write, :value {val}}}\n".format(id=client_id, val=value))
            return None
        majority_addresses = []
        for index in server_granting_locks:
            majority_addresses.append(addresses[index-1])

        latest_item = query_majority_servers(key, majority_addresses, session)

        # Create a request payload
        payload = {
            'key': key,
            'value': value,
            'ts': {
                'id': client_id,
                'integer': latest_item.get("ts", {}).get("integer", 0) + 1
            }
        }
        # Initialize list of write API calls, to send updated values to all servers, sent simultaneously
        request_futures = [session.post(address.rstrip() + "kv/write", json=payload) for address in addresses]
        # Receive responses from majority servers
        count=0
        # Break after receiving responses from the majority quorum
        for future in as_completed(request_futures):
            count += 1
            if(count <= final_count):
                print(future.result())
            else:
                print("Majority ACKs received")
                break
        # Release locks
        release_all_locks(key, session)
    log_output(str(time.time()) + ' : ' +"{{:process {id}, :type :ok, :f :write, :value {val}}}\n".format(id=client_id, val=value))
    return "Success"


def read(key):
    count = 0
    server_granting_locks = []
    # Initialize future session for creating asynchronous HTTP calls
    with FuturesSession() as session:

        # Acquire read locks from majority and save their server IDs in an array
        lock_futures = [session.get(address.rstrip() + "kv/blocking/acquire_lock/{}".format(key), params={'id': client_id}) for address in addresses]
        # Handle the calls as they are completed, breaking when the majority number has been reached
        for future in as_completed(lock_futures):
            count += 1
            if(count <= final_count):
                server_granting_locks.append(future.result().json())
                print(future.result().json())
            else:
                break
        server_granting_locks = [x['result'] for x in server_granting_locks]
        if False in server_granting_locks:
            print('Unable to acquire lock from majority. Please try again.')
            # Release locks
            release_all_locks(key, session)
            log_output(str(time.time()) + ' : ' +"{{:process {id}, :type :fail, :f :read, :value nil}}\n".format(id=client_id))
            return None
        majority_addresses = []
        for index in server_granting_locks:
            majority_addresses.append(addresses[index-1])


        # Get the item with the latest timestamp
        latest_item = query_majority_servers(key, majority_addresses, session)
        print(latest_item)

        # Update servers with latest item
        # Create a payload with the latest item
        payload = {
            'key': latest_item['key'],
            'value': latest_item['value'],
            'ts': {
                'id': latest_item.get("ts", {}).get("id", 0),
                'integer': latest_item.get("ts", {}).get("integer", 0)
            }
        }

        # Initialize list of write API calls, to send the latest item to all servers, sent simultaneously
        request_futures = [session.post(address.rstrip() + "kv/write", json=payload) for address in addresses]
        count=0
        # Handle the calls as they are completed, breaking when the majority number has been reached
        for future in as_completed(request_futures):
            count += 1
            if(count <= final_count):
                print(future.result())
            else:
                print("Majority ACKs received")
                break

        # Release locks
        release_all_locks(key, session)

        # Return the latest item's value
        log_output(str(time.time()) + ' : ' +"{{:process {id}, :type :ok, :f :read, :value {val}}}\n".format(id=client_id, val=latest_item['value']))
        return latest_item['value']


def log_output(log):
    edn_file = open(str(client_id)+'abd_log.edn', 'a+')
    edn_file.write(log)
    edn_file.close()


while True:
    print("Enter what you would like to do: ")
    print(" 1. Store/update a key,value \n 2. Read a key value \n 3. Exit \n 4. Random Run")
    message = 4  # Take in the option for process to be executed

    if 0 < message < 5:

        if message == 1:
            # Input for key,value to be stored/ updated at datastore
            key = input("Enter key name: ")
            value = input("Enter value/message to be stored against key: ")
            log_output("{{:process {id}, :type :invoke, :f write, :value {val}}}".format(id=client_id, val=value))
            status = write(key, value)
            if status:
                log_output("{{:process {id}, :type :ok, :f write, :value {val}}}".format(id=client_id, val=value))
            else:
                log_output("{{:process {id}, :type :fail, :f write, :value {val}}}".format(id=client_id, val=value))
            print(status)

        elif message == 2:
            # Enter key for search at data store
            key = input("Enter key name to be read: ")
            log_output("{{:process {id}, :type :invoke, :f read, :value nil}}".format(id=client_id))
            value = read(key)
            if value:
                log_output("{{:process {id}, :type :ok, :f read, :value {val}}}".format(id=client_id, val=value))
            else:
                log_output("{{:process {id}, :type :fail, :f read, :value nil}}".format(id=client_id))
            print("Value read for Key: ", key, " is Value: ", value)

        elif message == 3:
            print("End of execution session")
            break

        elif message == 4:
            for i in range(20):
                op = random.choice([1, 2])
                if op == 1:
                    value = random.randrange(1, 1000)
                    log_output(str(time.time()) + ' : ' +"{{:process {id}, :type :invoke, :f :write, :value {val}}}\n".format(id=client_id, val=value))
                    status = write('test', value)
                    print(status)
                else:
                    log_output(str(time.time()) + ' : ' +"{{:process {id}, :type :invoke, :f :read, :value nil}}\n".format(id=client_id))
                    value = read("test")
                    print("Value read for Key: ", "test", " is Value: ", value)
            break

    else:
        print("Invalid Option, try again")
