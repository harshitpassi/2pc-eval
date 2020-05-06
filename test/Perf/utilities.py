from time import sleep

# Reusable Utility functions


# Hihger order function for backoff
def retry_with_backoff(meth, key, value=None):
    attempts = int(input("Enter number of times operation should be re-attempted with backoff: "))
    for i in range(attempts):
        # Write case
        if value:
            status = meth(key, value)
        # Read case
        else:
            status = meth(key)
        if status:
            return status
        sleep(i*1)
    return None
