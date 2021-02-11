import datetime
import os
import logging
import traceback

from flask import jsonify
from pymongo import MongoClient

from app import app
from app.helpers import update_stats_db, insert_csvs_into_db, get_collections, get_stats_dict


@app.route('/')
def hello_world():
    return '/load to load the files \n\n  /stats/v1/measure to view stats'


@app.route('/load/')
def load_data():
    """
    loads the new files which were added after the latest timestamp of inserted files
    """
    try:
        collection, stats_collection, file_name_collection = get_collections()
        stats_dict = get_stats_dict(stats_collection)
        start_time = datetime.datetime.now()
        insert_csvs_into_db(collection, stats_dict, file_name_collection)
        update_stats_db(start_time, stats_collection, stats_dict)
        return 'data inserted', 201
    except Exception as e:
        logging.error('Error occurred! ', e)
        traceback.print_exc()
        return "Error", 500


@app.route('/stats/v1/measure')
def get_stats():
    """
    returns the time and count stats
    """
    try:
        client = MongoClient(os.environ.get('CONNECTION_STRING'))
        database = client['YOUR_DB_NAME']
        stats_collection = database['stats_collection']
        stats_cursor = stats_collection.find({})
        for i in stats_cursor:
            d = dict(i)
            del d['_id']
            return jsonify(d), 201
        return jsonify({}), 404
    except Exception as e:
        logging.error('Error occurred! ', e)
        traceback.print_exc()
        return "Error", 500
