from flask import Flask
from pymongo import MongoClient
import pandas as pd
from os import listdir
from os.path import isfile, join
from os import walk
import os
import re
import datetime

app = Flask(__name__)


@app.route('/')
def hello_world():
    return '/load to load the files \n  /stats/v1/measure to view stats'








if __name__ == "__main__":
    app.run(debug=True)
