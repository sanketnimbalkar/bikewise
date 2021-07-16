from copytohdfs import *
import os
from util import *


def main():
    #---Get all variables from conf file
    conf = get_conf()    
    file_format = conf.get('FILE_FORMAT')    
    dir_path = conf.get('DIR_PATH')
    url = conf.get('URL')
    bookmark_path = conf.get('BOOKMARK_PATH')
    hdfs_path = conf.get('HDFS_PATH')
    
    #---Get Status from bookmark file
    bookmark_data = get_bookmark(bookmark_path)
    for key in bookmark_data:
        filedate=key
        filestatus=bookmark_data[key]
        
    #---Copy file to HDFS
    if filestatus == "Downloaded" and os.path.exists(f"{dir_path}/{filedate}.json"):
        copy_from_local(dir_path,filedate,bookmark_path,hdfs_path)
    
if __name__ == '__main__':
    main()