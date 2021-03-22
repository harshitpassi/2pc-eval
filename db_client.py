from concurrent.futures import as_completed
from requests_futures.sessions import FuturesSession
from utilities import retry_with_backoff
import math
import random
import time
import csv

# Asking for unique client ID
print("Enter client ID:")
client_id = int(input())

# Read the config file for a list of addresses for all the servers
f = open("config", "r", encoding="utf-8")
addresses = f.readlines()
num_servers = len(addresses)
final_count = 0

# Address to be referred for current transaction
refer_address = []

# Initialize the count of the majority quorum needed for operations
if(num_servers % 2 == 0):
    final_count = num_servers / 2 + 1
else:
    final_count = math.ceil(num_servers / 2)

# Handler function to release acquired locks


def release_all_locks(key_list, session):
    # Release locks
    unlock_payload = {
        'keys': key_list
    }
    unlock_futures = [
        session.post(
            address.rstrip() +
            "kv/release_locks/",
            json=unlock_payload) for address in addresses]
    count = 0
    for future in as_completed(unlock_futures):
        try:
            count += 1
            if(count <= final_count):
                print(future.result())
            else:
                print("Majority ACKs received")
                break
        except BaseException:
            count -= 1
            print('Server unavailable.')
            continue
    refer_address.clear()

# Try to acquire locks from a majority of servers, return addresses of
# servers that have granted locks if granted, None otherwise


def acquire_locks(key_list, session):
    lock_payload = {
        'id': client_id,
        'keys': key_list
    }
    count = 0
    server_granting_locks = []
    lock_futures = [
        session.post(
            address.rstrip() +
            "kv/acquire_locks/",
            json=lock_payload) for address in addresses]
    # Handle the calls as they are completed, breaking when the majority
    # number has been reached
    for future in as_completed(lock_futures):
        try:
            count += 1
            if(count <= final_count):
                server_granting_locks.append(future.result().json())
                print(future.result().json())
            else:
                break
        except BaseException:
            count -= 1
            print('Server unavailable.')
            continue
    server_granting_locks = [x['result'] for x in server_granting_locks]
    if False in server_granting_locks:
        print('Unable to acquire lock from majority. Please try again.')
        # Release locks
        release_all_locks(key_list, session)
        refer_address.append(None)
    majority_addresses = []
    for index in server_granting_locks:
        majority_addresses.append(addresses[index - 1])
    refer_address.append(majority_addresses[0])


def write(key, value):
    # Initialize future session for creating asynchronous HTTP calls
    with FuturesSession(adapter_kwargs={'max_retries': 0}) as session:
        # Acquire write locks from majority and save their server IDs in an
        # array
        key_list = []
        key_list.append(key)
        acquire_locks(key_list, session)

        if refer_address[0] is None:
            return None

        # Create a request payload
        payload = {
            'key': key,
            'value': value
        }

        # Initialize list of write API calls, to send updated values to all
        # servers, sent simultaneously
        request_futures = [
            session.post(
                address.rstrip() + "kv/write",
                json=payload) for address in refer_address]
        # Receive responses from majority servers
        count = 0
        # Break after receiving responses from the majority quorum
        for future in as_completed(request_futures):
            try:
                count += 1
                if(count <= 1):
                    print(future.result())
                else:
                    print("Majority ACKs received")
                    break
            except BaseException:
                count -= 1
                print('Server unavailable.')
                continue
        # Release locks
        release_all_locks(key_list, session)
    return "Success"


def read(key):
    # Initialize future session for creating asynchronous HTTP calls
    with FuturesSession(adapter_kwargs={'max_retries': 0}) as session:

        # Acquire read locks from majority and save their server IDs in an
        # array
        key_list = []
        key_list.append(key)
        acquire_locks(key_list, session)

        if refer_address[0] is None:
            return None

        # Initialize list of read API calls, to send updated values to all
        # servers, sent simultaneously
        response_futures = [
            session.get(
                address.rstrip() +
                "kv/read/{}".format(key)) for address in refer_address]
        # Receive responses from majority servers
        count = 0
        result = ''
        # Break after receiving responses from the majority quorum
        for future in as_completed(response_futures):
            try:
                count += 1
                if(count <= 1):
                    result = future.result().json()
                else:
                    print("Majority ACKs received")
                    break
            except BaseException:
                count -= 1
                print('Server unavailable.')
                continue
        # Release locks
        release_all_locks(key_list, session)
    return result

while True:
    print("Enter what you would like to do: ")
    print(" 1. Store/update a key,value \n 2. Read a key value \n 3. Exit \n 4. Random Run \n 5. Throughput and Latency Evaluation ")
    # Take in the option for process to be executed
    message = int(input())
    if 0 < message < 6:
        if message == 1:
            # Input for key,value to be stored/ updated at datastore
            key = input("Enter key name: ")
            value = input("Enter value/message to be stored against key: ")
            status = write(key, value)
            # None returned (locked) retry writes with backoff
            if not status:
                status = retry_with_backoff(write, key, value)
                if not status:
                    print("Operation unsucessful")
                else:
                    print(status)
            else:
                print(status)
        elif message == 2:
            # Enter key for search at data store
            key = input("Enter key name to be read: ")
            value = read(key)
            # Read unsuccessful, retry reads with backoff
            if not value:
                value = retry_with_backoff(read, key)
                if not value:
                    print("Operation unsucessful")
                else:
                    print("Value read for Key: ", key, " is Value: ", value)
            else:
                print("Value read for Key: ", key, " is Value: ", value)
        elif message == 3:
            print("End of execution session")
            break
