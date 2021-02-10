import os
import re
from os import walk

import pandas as pd
import datetime

from pymongo import MongoClient


def csv_to_json(filename):
    """
    converts csv to json using pandas
    """
    data = pd.read_csv(filename)
    return data.to_dict('records')


def extract_timestamp(name):
    """
    extracts timestamp from the file name
    """
    match = re.search(re.compile('\d{4}-\d{2}-\d{2}-\d{2}:\d{2}'), name)
    if match:
        date = datetime.datetime.strptime(match.group(), '%Y-%m-%d-%H:%M')
        return date
    else:
        match = re.search(re.compile('\d{4}-\d{2}-\d{2} \d{2}:\d{2}'), name)
        date = datetime.datetime.strptime(match.group(), '%Y-%m-%d %H:%M')
        return date


def update_stats_db(start_time, stats_collection, stats_dict):
    stats_dict['Time_taken'] = stats_dict['Time_taken'] + (datetime.datetime.now() - start_time).seconds
    x = stats_collection.delete_many({})
    stats_collection.insert_one(stats_dict)


def update_time_db(time, time_collection):
    if time != datetime.datetime.min:
        time_collection.delete_many({})
        time_dict = dict()
        time_dict['time'] = time
        time_collection.insert_one(time_dict)


def insert_csvs_into_db(collection, last_inserted_time, stats_dict, time):
    for (dirpath, dirnames, filenames) in walk(os.environ.get(
            'CSV_FOLDER')):  # for local development SFTP code should be added for SFTP remote connections
        for filename in filenames:
            timestamp = extract_timestamp(filename)
            if timestamp > last_inserted_time:  # compae with highest timestamp
                filepath = (str(dirpath + os.path.sep + filename))
                if timestamp > time:
                    time = timestamp  # update timestamp to be posted in time_collection
                collection.insert_many(csv_to_json(str(filepath)))
                stats_dict['files_count'] += 1
    return time


def get_collections():
    client = MongoClient(os.environ.get('CONNECTION_STRING'))
    database = client['YOUR_DB_NAME']
    collection = database['your_collection']  # maintains the dataabese
    time_collection = database['time_collection']  # maintains the timestamp of the last file added
    stats_collection = database['stats_collection']  # maintains the stats of time and count
    return collection, stats_collection, time_collection


def get_last_inserted_time(time_collection):
    last_inserted_time = datetime.datetime.min
    cursor = time_collection.find({})
    for document in cursor:
        last_inserted_time = document['time']  # get the highest timestamp of the already inserted file
    return last_inserted_time


def get_stats_dict(stats_collection):
    stats_cursor = stats_collection.find({})
    stats_dict = {'files_count': 0, 'Time_taken': 0}
    for document in stats_cursor:
        stats_dict['files_count'] = document['files_count']
        stats_dict['Time_taken'] = document['Time_taken']
    return stats_dict