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

def dedupe_results(results):
    """Remove duplicate entries from listResults and mapResults based on zpid (property ID).
    
    Each array is deduped independently (removing duplicates within that array).
    Both arrays can share the same zpids — that's expected and correct.
    
    Returns the deduplicated results dict.
    """
    # Dedupe listResults independently
    seen_list_zpids = set()
    deduped_list = []
    for item in results.get("listResults", []):
        zpid = item.get("zpid")
        if zpid and zpid not in seen_list_zpids:
            seen_list_zpids.add(zpid)
            deduped_list.append(item)
    
    # Dedupe mapResults independently (separate zpid tracking)
    seen_map_zpids = set()
    deduped_map = []
    for item in results.get("mapResults", []):
        zpid = item.get("zpid")
        if zpid and zpid not in seen_map_zpids:
            seen_map_zpids.add(zpid)
            deduped_map.append(item)
    
    results["listResults"] = deduped_list
    results["mapResults"] = deduped_map
    return results