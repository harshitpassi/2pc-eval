from concurrent.futures import as_completed
from requests_futures.sessions import FuturesSession
import math

#TO-DO: Creating a client using this API. Also need to add client ID implementation.

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

def write(key, value):
    count = 0
    responses = []
    # Initialize future session for creating asynchronous HTTP calls
    with FuturesSession() as session:
        # Initialize list of read API calls, to get current timestamps in each server, all sent simultaneously
        response_futures = [session.get(address.rstrip() + "kv/read/{}".format(key)) for address in addresses]
        # Handle the calls as they are completed, breaking when the majority number has been reached
        for future in as_completed(response_futures):
            count += 1
            if(count <= final_count):
                responses.append(future.result().json())
            else:
                break
        # Get the maximum timestamp from the received responses
        timestamps = [response.get("ts", 0) for response in responses]
        latest_timestamp = max(timestamps)
        print(timestamps)
        # Create a request payload with an updated timestamp
        payload = {
            'key': key,
            'value': value,
            'ts': latest_timestamp+1
        }
        # Initialize list of write API calls, to send updated values to all servers, sent simultaneously
        request_futures = [session.post(address.rstrip() + "kv/write", params=payload) for address in addresses]
        count=0
        # Break after receiving responses from the majority quorum
        for future in as_completed(request_futures):
            count += 1
            if(count <= final_count):
                print(future.result())
            else:
                print("Majority ACKs received")
                break
    return "Success"

def read(key):
    count = 0
    responses = []
    # Initialize future session for creating asynchronous HTTP calls
    with FuturesSession() as session:
        # Initialize list of read API calls, to get current timestamps in each server, all sent simultaneously
        response_futures = [session.get(address.rstrip() + "kv/read/{}".format(key)) for address in addresses]
        # Handle the calls as they are completed, breaking when the majority number has been reached
        for future in as_completed(response_futures):
            count += 1
            if(count <= final_count):
                responses.append(future.result().json())
            else:
                break
        # Get the item with the latest timestamp
        latest_item = max(responses, key=lambda x:x.get('ts', 0))
        print(latest_item)
        # Create a payload with the latest item
        payload = {
            'key': latest_item['key'],
            'value': latest_item['value'],
            'ts': latest_item['ts']
        }
        # Initialize list of write API calls, to send the latest item to all servers, sent simultaneously
        request_futures = [session.post(address.rstrip() + "kv/write", params=payload) for address in addresses]
        count=0
        # Handle the calls as they are completed, breaking when the majority number has been reached
        for future in as_completed(request_futures):
            count += 1
            if(count <= final_count):
                print(future.result())
            else:
                print("Majority ACKs received")
                break
        # Return the latest item's value
        return latest_item['value']

print(read(0))