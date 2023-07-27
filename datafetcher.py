"""
this module is used to fetch data from the Environment Agency API.

 """

import datetime
import json
import requests
from s3fs import S3FileSystem


def fetch(url):
    """Fetch data from url and return fetched JSON object

    Args:
        url (str): url to be fetched

    Returns:
        dict: Json string fetched
    """
    r = requests.get(url)
    data = r.json()
    return data


def dump(data, filename,s3):
    """Save JSON object to file

    Args:
        data (dict): Json string to be saved
        filename (str): Path to file
    """
    
    domain = "YOUR_S3_BUCKET_NAME"
    with s3.open(domain+filename, "w") as f:
        json.dump(data, f)


def load(filename):
    """Load JSON object from file

    Args:
        filename (str): Path to file

    Returns:
        dict: Json string
    """
    with open(filename, "r") as f:
        data = json.load(f)
    return data


def fetch_station_data():
    """Fetch data from Environment agency for all active river level
    monitoring stations via a REST API and return retrieved data as a
    JSON object.

    Fetched data is dumped to a cache file so on subsequent call it can
    optionally be retrieved from the cache file. This is faster than
    retrieval over the Internet and avoids excessive calls to the
    Environment Agency service.

    Args:
        use_cache (bool, optional): Whether to use cached data


    """

    url = "http://environment.data.gov.uk/flood-monitoring/id/stations?status=Active&_view=full"  # noqa

    

    # Attempt to load station data from file, otherwise fetch over
    
    s3 = S3FileSystem()
    # Fetch and dump to file
    data = fetch(url)
    for count, i in enumerate(data["items"]):
        dump(i, "/stations/station_{}.json".format(count),s3)
    return data



def fetch_latest_water_level_data(use_cache=False):
    """Fetch latest levels from all 'measures'. Returns JSON object

    Args:
        use_cache (bool, optional): Whether to use cached data

    Returns:
        dict: Json string
    """
    s3 = S3FileSystem()
    # URL for retrieving data
    url = "http://environment.data.gov.uk/flood-monitoring/id/measures?latest"  # noqa

    data = fetch(url)
    for count, i in enumerate(data["items"]):
        dump(i, "/measure/measure_{}.json".format(count),s3)



def fetch_measure_levels(measure_id, dt):
    """Fetch measure levels from latest reading and going back a period
    dt. Return list of dates and a list of values.

    Args:
        measure_id (str): measure_id of the specified station
        dt (DateTime Object): Period of time

    """

    # Current time (UTC)
    now = datetime.datetime.utcnow()

    s3 = S3FileSystem()
    
    # Start time for data
    start = now - dt

    # Construct URL for fetching data
    url_base = "http://environment.data.gov.uk/flood-monitoring/id/measures/"+measure_id
    url_options = "/readings/?_sorted&since=" + start.isoformat() + "Z"
    url = url_base + url_options

    # Fetch data
    data = fetch(url)
    
    for count, i in enumerate(data["items"]):
        dump(i, "/measure/measure_.json".format(count),s3)



if __name__ == "__main__":
    data=fetch_station_data()