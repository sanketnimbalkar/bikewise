from util import *
from read import *
import os


def main():
    #env = os.environ.get('ENVIRON')
    env = 'PROD'
    
    #---Get spark session
    #spark = get_spark_session(env,'BikeWise - Getting Started')
    
    #---Get all variables from conf file
    conf =get_conf()    
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
    
    #---Read data and write to local
    if filestatus == "NotYetDownloaded" and not os.path.exists(f"{dir_path}/{filedate}.json"):
        print("To Download")
        #read_from_api(spark,url,file_format,filedate,dir_path,bookmark_path)

    #stop_spark_session(spark)

if __name__ == '__main__':
    main()
