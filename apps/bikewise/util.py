from pyspark.sql import SparkSession
import findspark
import yaml
import getpass
import json

def get_spark_session(env,app_name):
    findspark.init('/opt/spark2-client')
    username = getpass.getuser()
    if env == 'DEV':
        spark = SparkSession. \
            builder. \
            config("spark.sql.warehouse.dir", f"/user/itv000547/warehouse"). \
            enableHiveSupport(). \
            master('local'). \
            appName(app_name). \
            getOrCreate()
        return spark
    elif env == 'PROD':
        spark = SparkSession. \
            builder. \
            config("spark.sql.warehouse.dir", f"/user/itv000547/warehouse"). \
            enableHiveSupport(). \
            master('yarn'). \
            appName(app_name). \
            getOrCreate()
        return spark

def stop_spark_session(spark):
    spark.stop()
    
def get_conf():
    a_yaml_file = open("/home/itv000547/airflow-examples/apps/bikewise/conf.yml")
    conf = yaml.load(a_yaml_file, Loader=yaml.FullLoader)
    return conf['dev']

    
def get_bookmark(bookmark_path):
    jsonFile = open(bookmark_path, "r+")
    bookmark_data = json.load(jsonFile)
    jsonFile.close()
    return bookmark_data
    
def update_bookmark(bookmark_path,filedate,filestatus):
    bookmark_data = {}
    jsonFile = open(bookmark_path, "w")
    bookmark_data[filedate] = filestatus
    jsonFile.write(json.dumps(bookmark_data))
    jsonFile.close()
    
def update_bookmark_date(bookmark_path,newfiledate):
    bookmark_data = {}
    jsonFile = open(bookmark_path, "w")
    bookmark_data[newfiledate] = "NotYetDownloaded"
    jsonFile.write(json.dumps(bookmark_data))
    