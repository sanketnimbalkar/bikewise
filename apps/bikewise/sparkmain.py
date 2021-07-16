from util import *
from creatingsparkmetastore import *

def main():
#     env = os.environ.get('ENVIRON')
    env = 'PROD'
    spark = get_spark_session(env,'BikeWise - Getting Started')
    
    #---Get all variables from conf file
    conf = get_conf()    
    file_format = conf.get('FILE_FORMAT')    
    dir_path = conf.get('DIR_PATH')
    url = conf.get('URL')
    bookmark_path = conf.get('BOOKMARK_PATH')
    hdfs_path = conf.get('HDFS_PATH')
    database_path = conf.get('DATABASE_PATH')
    
    #---Get Status from bookmark file
    bookmark_data = get_bookmark(bookmark_path)
    for key in bookmark_data:
        filedate=key
        filestatus=bookmark_data[key]
      
    if filestatus == "Copied":
        data_into_table(spark,hdfs_path,filedate,database_path,bookmark_path)    
    
    
if __name__ == '__main__':
    main()
