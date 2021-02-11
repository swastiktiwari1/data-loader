import datetime
import os
import traceback
from os import walk

import pandas as pd
from pymongo import MongoClient
import logging


def csv_to_json(filename):
    """
    converts csv to json using pandas
    """
    try:
        logging.info("converting csv", filename, "to json")
        data = pd.read_csv(filename)
        return data.to_dict('records')
    except Exception as e:
        logging.error('Error occurred! ', e)
        traceback.print_exc()


def update_stats_db(start_time, stats_collection, stats_dict):
    logging.info("Updating stats in the db")
    stats_dict['Time_taken'] = stats_dict['Time_taken'] + (datetime.datetime.now() - start_time).seconds
    stats_collection.delete_many({})
    stats_collection.insert_one(stats_dict)
    logging.info("Updated stats in the db")


def insert_csvs_into_db(collection, stats_dict, file_name_collection):
    try:
        for (dir_path, dir_names, file_names) in walk(os.environ.get(
                'CSV_FOLDER')):  # for local development SFTP code should be added for SFTP remote connections
            for file_name in file_names:
                filepath = (str(dir_path + os.path.sep + file_name))
                if file_name_collection.find({"file_name": file_name}).count() == 0:
                    collection.insert_many(csv_to_json(str(filepath)))
                    file_name_collection.insert_one({"file_name": file_name})
                    stats_dict['files_count'] += 1
                    logging.info("File", file_name, "inserted into db")
                else:
                    logging.info("File", file_name, "already present in db")
    except Exception as e:
        logging.error('Error occurred! ', e)
        traceback.print_exc()


def get_collections():
    try:
        logging.info("Getting collections from database")
        client = MongoClient(os.environ.get('CONNECTION_STRING'))
        database = client['YOUR_DB_NAME']
        collection = database['your_collection']  # maintains the database
        stats_collection = database['stats_collection']  # maintains the stats of time and count
        if 'file_name_collection' in database.list_collection_names():
            file_name_collection = database['file_name_collection']
        else:
            file_name_collection = database['file_name_collection']
            file_name_collection.create_index("file_name")
        return collection, stats_collection, file_name_collection
    except Exception as e:
        logging.error('Error occurred! ', e)
        traceback.print_exc()


def get_stats_dict(stats_collection):
    stats_cursor = stats_collection.find({})
    stats_dict = {'files_count': 0, 'Time_taken': 0}
    for document in stats_cursor:
        stats_dict['files_count'] = document['files_count']
        stats_dict['Time_taken'] = document['Time_taken']
    logging.info("stats dict", stats_dict)
    return stats_dict
