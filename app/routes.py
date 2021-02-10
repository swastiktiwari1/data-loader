from flask import jsonify
from pymongo import MongoClient
import os

import datetime
from app import app
from app.helpers import update_stats_db, update_time_db, insert_csvs_into_db, get_collections, get_last_inserted_time, \
    get_stats_dict


@app.route('/')
def hello_world():
    return '/load to load the files \n\n  /stats/v1/measure to view stats'


@app.route('/load/')
def load_data():
    """
    loads the new files which were added after the latest timestamp of inserted files
    """
    collection, stats_collection, time_collection = get_collections()
    time = datetime.datetime.min

    stats_dict = get_stats_dict(stats_collection)
    last_inserted_time = get_last_inserted_time(time_collection)
    start_time = datetime.datetime.now()
    time = insert_csvs_into_db(collection, last_inserted_time, stats_dict, time)
    update_time_db(time, time_collection)
    update_stats_db(start_time, stats_collection, stats_dict)
    return 'data inserted', 201


@app.route('/stats/v1/measure')
def get_stats():
    """
    returns the time and count stats
    """
    client = MongoClient(os.environ.get('CONNECTION_STRING'))
    database = client['YOUR_DB_NAME']
    stats_collection = database['stats_collection']
    stats_cursor = stats_collection.find({})
    for i in stats_cursor:
        d = dict(i)
        del d['_id']
        return jsonify(d), 201
    return jsonify({}), 404
