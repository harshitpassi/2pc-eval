from time import sleep

# Reusable Utility functions


# Hihger order function for backoff
def retry_with_backoff(meth, key_list, id):
    count = 0
    while True:
        status = meth(key_list, id)
        if status != None:
            return status
        count += 1
        sleep(count*0.01)
    return None
