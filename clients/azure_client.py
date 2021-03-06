from concurrent.futures import as_completed, ThreadPoolExecutor
from requests_futures.sessions import FuturesSession
import math
import random
import time
import csv
import requests
import threading
import functools
import pymongo

uri = "mongodb://cse-541-test:Xbk45r0The0816dESz47ms6FJCob51Dlu6U0pLRpAjFICb6MmkhTwTL0i49VxOALpRy9m87d7J7gQWopKfNgGQ==@cse-541-test.mongo.cosmos.azure.com:10255/?ssl=true&retrywrites=false&replicaSet=globaldb&maxIdleTimeMS=120000&appName=@cse-541-test@"
client = pymongo.MongoClient(uri)
db = client.bank
collection = db.accounts


latency_val = []

def perf_run(session):
    global latency_val
    global client
    global db
    global collection
    
    t_start = time.perf_counter()
    with session.start_transaction():
        from_account = {"number": 1}
        to_account = {"number": 2}

        from_update = { "$inc": { "balance": -1 } }
        to_update = { "$inc": { "balance": 1 } }

        collection.update_one(from_account, from_update)
        collection.update_one(to_account, to_update)
    t_end = time.perf_counter()
    with threading.Lock():
        latency_val.append((t_end - t_start)*1000)
    #print(latency_val)


def thread_helper(thread_num, num_runs):
    print("Thread {} started!".format(thread_num))
    with client.start_session() as session:
        for index in range(num_runs):
            perf_run(session)
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
    print(" 1. Single Threaded Performance Test \n 2. Multithreaded Performance Test \n 3. Exit \n ")
    # Take in the option for process to be executed
    message1 = int(input())
    while message1 < 3:
        if message1 == 1:
            with client.start_session() as session:
                latency_val.clear()
                num_runs = int(input('How many test runs do you want? '))
                th_start = time.perf_counter()
                while len(latency_val) != num_runs:
                    perf_run(session)
                th_end = time.perf_counter()
                throughput = num_runs/(th_end-th_start)
                print("Throughput: {}".format(throughput))
                print("Minimum latency: {}".format(min(latency_val)))
                print("Maximum latency: {}".format(max(latency_val)))
                print("Average latency: {}".format(sum(latency_val)/len(latency_val)))
                write_output('single_thread_azure')
                latency_val.sort()
                perc_95 = functools.partial(percentile, percent=0.95)
                perc_99 = functools.partial(percentile, percent=0.99)
                print("95th percentile latency: {}".format(perc_95(latency_val)))
                print("99th percentile latency: {}".format(perc_99(latency_val)))
            break
        elif message1 == 2:
            latency_val.clear()
            threads = list()
            num_runs = int(input('How many test runs do you want? '))
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
            write_output('multi_threaded_azure')
            latency_val.sort()
            perc_95 = functools.partial(percentile, percent=0.95)
            perc_99 = functools.partial(percentile, percent=0.99)
            print("95th percentile latency: {}".format(perc_95(latency_val)))
            print("99th percentile latency: {}".format(perc_99(latency_val)))
            break
        else:
            break

