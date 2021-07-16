from util import *
import os

def write_local(bikewise_df,dir_path,bookmark_path,filedate):
    bikewise_df.to_json(f"{dir_path}/{filedate}.json", orient="records")
    
    if os.path.exists(f"{dir_path}/{filedate}.json"):
        filestatus = "Downloaded"
        update_bookmark(bookmark_path,filedate,filestatus)