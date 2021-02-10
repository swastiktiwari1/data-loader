
from pymongo import MongoClient

from os import listdir
from os.path import isfile, join
from os import walk
import os

import datetime
from app import app
from app import helpers
@app.route('/')
def hello_world():
    return '/load to load the files \n  /stats/v1/measure to view stats'

@app.route('/load/')
def load_data():
    """
    loads the new files which were added after the latest timestamp of inserted files
    """
    client = MongoClient("mongodb+srv://user:forcabarca@cluster0.t7bhf.mongodb.net/ciq?retryWrites=true&w=majority")
    db = client.test
    database = client['YOUR_DB_NAME']
    collection = database['your_collection']  # maintains the dataabese
    time_collection = database['time_collection']  # maintains the timestamp of the last file added
    stats_collection = database['stats_collection']  # maintains the stats of time and count
    time = datetime.datetime.min
    stats_cursor = stats_collection.find({})
    last_inserted_time = datetime.datetime.min
    cursor = time_collection.find({})
    stats_dict = {'files_count': 0, 'Time_taken': 0}
    for documet in stats_cursor:
        stats_dict['files_count'] = documet['files_count']
        stats_dict['Time_taken'] = documet['Time_taken']
    for document in cursor:
        last_inserted_time = document['time']  # get the highest timestamp of the alredy inserted file
    start_time = datetime.datetime.now()
    for (dirpath, dirnames, filenames) in walk(
            "/home/swastik/commerceIQ/csvs"):  # for local development SFTP code should be added for SFTP remote connections
        for filename in filenames:
            timestamp = helpers.extract_timestamp(filename)
            if timestamp > last_inserted_time:  # compae with highest timestamp
                filepath = (str(dirpath + os.path.sep + filename))
                if (timestamp > time):
                    time = timestamp  # update timestamp to be posted in time_collection
                collection.insert_many(csv_to_json(str(filepath)))
                stats_dict['files_count'] += 1
    if time != datetime.datetime.min:
        x = time_collection.delete_many({})
        time_dict = dict()
        time_dict['time'] = time
        time_collection.insert_one(time_dict)
    stats_dict['Time_taken'] = stats_dict['Time_taken'] + (datetime.datetime.now() - start_time).seconds
    x = stats_collection.delete_many({})
    stats_collection.insert_one(stats_dict)
    return 'data inserted'


@app.route('/stats/v1/measure')
def get_stats():
    """
    returns the time and count stats
    """
    client = MongoClient("mongodb+srv://user:forcabarca@cluster0.t7bhf.mongodb.net/ciq?retryWrites=true&w=majority")
    database = client['YOUR_DB_NAME']
    stats_collection = database['stats_collection']
    stats_cursor = stats_collection.find({})
    for i in stats_cursor:
        d = dict(i)
        del d['_id']
        return d
    return {}