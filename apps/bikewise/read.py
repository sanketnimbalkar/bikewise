import requests
import json
from gettimestamp import *
import datetime as dt
from write import *


def read_from_api(spark,url,file_format,filedate,dir_path):
    file_date = dt.datetime.strptime(filedate, '%Y-%m-%d')
    date_values = get_timestamp(file_date)
    occuredafter = date_values[0]
    occuredbefore = date_values[1]
    bikewise = json.loads(requests.get(f'{url}&occurred_before={occuredbefore}&occurred_after={occuredafter}').content.decode('utf-8'))['incidents']
    #bikewise_df = pd.json_normalize(bikewise)
    bikewise_df = spark.read.\
        format(file_format).\
        load(bikewise)
    write_local(bikewise_df,dir_path,bookmark_path,filedate)
