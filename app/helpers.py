import re
import pandas as pd
import datetime
def csv_to_json(filename, header=None):
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