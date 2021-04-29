from concurrent.futures import as_completed, ThreadPoolExecutor
from requests_futures.sessions import FuturesSession
from utilities import retry_with_backoff
import math
import random
import time
import csv
import requests
import threading
import functools

# Asking for unique client ID
print("Enter client ID:")
client_id = int(input())

# Read the config file for a list of addresses for all the servers
f = open("config", "r", encoding="utf-8")
addresses = f.readlines()
num_servers = len(addresses)
final_count = 0

# Initialize the count of the majority quorum needed for locking/unlocking
if(num_servers % 2 == 0):
    final_count = num_servers / 2 + 1
else:
    final_count = math.ceil(num_servers / 2)

# Handler function to release acquired locks
def release_all_locks(key_list, id):
    with FuturesSession(adapter_kwargs={'max_retries': 0}) as session:
        # Release locks
        unlock_payload = {
            'id':id,
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
                    pass
                    #print(future.result())
                else:
                    #print("Majority ACKs received")
                    break
            except BaseException:
                count -= 1
                print('Server unavailable.')
                continue
        #refer_address = None

# Try to acquire locks from a majority of servers, return addresses of
# servers that have granted locks if granted, None otherwise
def acquire_locks(key_list, id):
    with FuturesSession(adapter_kwargs={'max_retries': 0}) as session:
        lock_payload = {
            'id': id,
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
                    #print(future.result().json())
                else:
                    break
            except BaseException:
                count -= 1
                print('Server unavailable.')
                continue
        server_granting_locks = [x['result'] for x in server_granting_locks]
        if False in server_granting_locks:
            #print('Unable to acquire lock from majority. Please try again.')
            # Release locks
            release_all_locks(key_list, id)
            return None
        majority_addresses = []
        for index in server_granting_locks:
            majority_addresses.append(addresses[index - 1])
        #print('REFER ADDRESS: {}'.format(majority_addresses[0]))
        return majority_addresses[0]

def begin_transaction(transaction_address):
    if transaction_address is None:
        return None
    #print(transaction_address)
    final_address = transaction_address.rstrip()+"kv/begin_transaction"
    response=requests.get(final_address)
    #print(response.json())
    return response.json()

def commit_transaction(transaction_address):
    if transaction_address is None:
        return None
    response=requests.get(transaction_address.rstrip() + "kv/commit_transaction")
    #print(response.json())
    return response.json()


def write(key, value, transaction_address):
    if transaction_address is None:
        return None
    # Create a request payload
    payload = {
        'key': key,
        'value': value
    }
    # Send write request
    response = requests.post(transaction_address.rstrip() + "kv/write", json=payload)
    # Break after receiving responses from the majority quorum
    #print(response.json())
    return "Success"


def read(key, transaction_address):
    if transaction_address is None:
        return None
    # Send read response
    response=requests.get(transaction_address.rstrip() + "kv/read/{}".format(key))
    # Receive responses from majority servers
    result = response.json()
    return result

latency_val = []

def perf_run(thread_num):
    global latency_val
    count = 0
    key_list = []
    from_account = "1"
    to_account = "2"

    key_list.append(from_account)
    key_list.append(to_account)

    t_start = time.perf_counter()

    transaction_address = acquire_locks(key_list, thread_num)
    if not transaction_address:
        transaction_address = retry_with_backoff(acquire_locks, key_list, thread_num)
    begin_transaction(transaction_address)

    from_current_balance = int(read(from_account, transaction_address))
    to_current_balance = int(read(to_account, transaction_address))

    from_newval = from_current_balance - 1
    to_newval = to_current_balance + 1

    write(from_account, str(from_newval), transaction_address)
    write(to_account, str(to_newval), transaction_address)

    commit_transaction(transaction_address)
    release_all_locks(key_list, thread_num)

    t_end = time.perf_counter()
    with threading.Lock():
        latency_val.append((t_end - t_start)*1000)
    #print(latency_val)


def thread_helper(thread_num, num_runs):
    print("Thread {} started!".format(thread_num))
    for index in range(num_runs):
        perf_run(thread_num)
    print("Thread {} done!".format(thread_num))

def percentile(N, percent, key=lambda x:x):
    """
    Find the percentile of a list of values.

    @parameter N - is a list of values. Note N MUST BE already sorted.
    @parameter percent - a float value from 0.0 to 1.0.
    @parameter key - optional key function to compute value from each element of N.

    @return - the percentile of the values
    """
    if not N:
        return None
    k = (len(N)-1) * percent
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return key(N[int(k)])
    d0 = key(N[int(f)]) * (c-k)
    d1 = key(N[int(c)]) * (k-f)
    return d0+d1

def write_output(name):
    global latency_val
    with open(name, 'w') as f:
        write = csv.writer(f)
        write.writerow(range(len(latency_val)))
        write.writerow(latency_val)




while True:
    print("Enter what you would like to do: ")
    print(" 1. Begin Transaction \n 2. Single Threaded Performance Test \n 3. Multithreaded Performance Test \n 4. Exit \n ")
    # Take in the option for process to be executed
    message1 = int(input())
    transaction_address=''
    while message1 != 4:
        if message1 == 1:
            print(" 1. Transfer Money \n 2. View Account Balance \n")
            message = int(input())
            key = ''
            key_list = []
            if 0 < message < 3:
                if message == 1:
                    print('Beginning transaction')

                    # User Input for Account Numbers
                    from_account = input("Enter your Account Number: ")
                    to_account = input("Enter Recipient Account Number: ")

                    #Preparing lock acquisition request
                    key_list.append(from_account)
                    key_list.append(to_account)

                    # Acquire locks and begin transaction
                    transaction_address = acquire_locks(key_list, client_id)
                    begin_transaction(transaction_address)

                    # fetch current balance
                    from_current_balance = int(read(from_account, transaction_address))
                    print("Your current account balance is: ${}".format(from_current_balance))
                    to_current_balance = int(read(to_account, transaction_address))

                    transfer = int(input("How much do you want to transfer to Account Number {}? $".format(to_account)))

                    from_newval = from_current_balance - transfer
                    to_newval = to_current_balance + transfer

                    write(from_account, str(from_newval), transaction_address)
                    write(to_account, str(to_newval), transaction_address)

                    print('Committing transaction')
                    print(commit_transaction(transaction_address))

                    print('Releasing locks')
                    release_all_locks(key_list, client_id)
                    print("Transaction successfully committed.")
                elif message == 2:
                    # Enter key for search at data store
                    key = input("Enter your account number: ")
                    value = read(key, transaction_address)
                    print("Value read for Key: ", key, " is Value: ", value)
                else:
                    print("Invalid input")
                    break
        elif message1 == 2:
            latency_val.clear()
            num_runs = int(input('How many test runs do you want? '))
            th_start = time.perf_counter()
            while len(latency_val) != num_runs:
                perf_run(client_id)
            th_end = time.perf_counter()
            throughput = num_runs/(th_end-th_start)
            print("Throughput: {}".format(throughput))
            print("Minimum latency: {}".format(min(latency_val)))
            print("Maximum latency: {}".format(max(latency_val)))
            print("Average latency: {}".format(sum(latency_val)/len(latency_val)))
            write_output('single_thread_2pc')
            latency_val.sort()
            perc_95 = functools.partial(percentile, percent=0.95)
            perc_99 = functools.partial(percentile, percent=0.99)
            print("95th percentile latency: {}".format(perc_95(latency_val)))
            print("99th percentile latency: {}".format(perc_99(latency_val)))
            break
        elif message1 == 3:
            latency_val.clear()
            threads = list()
            num_runs = int(input('How many test runs do you want? (Per thread) '))
            th_start = time.perf_counter()
            for index in range(5):
                x = threading.Thread(target=thread_helper, args=(index,num_runs,))
                threads.append(x)
                x.start()
            for thread in threads:
                thread.join()
            th_end = time.perf_counter()
            throughput = (5*num_runs)/(th_end-th_start)
            print("Throughput: {}".format(throughput))
            print("Minimum latency: {}".format(min(latency_val)))
            print("Maximum latency: {}".format(max(latency_val)))
            print("Average latency: {}".format(sum(latency_val)/len(latency_val)))
            write_output('multi_threaded_2pc')
            latency_val.sort()
            perc_95 = functools.partial(percentile, percent=0.95)
            perc_99 = functools.partial(percentile, percent=0.99)
            print("95th percentile latency: {}".format(perc_95(latency_val)))
            print("99th percentile latency: {}".format(perc_99(latency_val)))
            break
        elif message1 == 4:
            break
    break

