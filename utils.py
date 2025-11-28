import time
import random


def gaussian_sleep(mean=4.0, std_dev=1.5, min_sleep=2.0, max_sleep=10.0):
    """Sleep for a random duration drawn from a Gaussian distribution.
    
    - `mean`: center of the distribution (seconds)
    - `std_dev`: standard deviation (seconds)
    - `min_sleep`: minimum sleep time (clamp lower bound)
    - `max_sleep`: maximum sleep time (clamp upper bound)
    
    Returns the actual sleep duration.
    """
    sleep_time = random.gauss(mean, std_dev)
    sleep_time = max(min_sleep, min(sleep_time, max_sleep))
    print(f"Sleeping for {sleep_time} seconds")
    time.sleep(sleep_time)
    return sleep_time
