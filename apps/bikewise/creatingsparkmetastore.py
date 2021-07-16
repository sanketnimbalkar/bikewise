import datetime
from util import *
from pyspark.sql.functions import *

def data_into_table(spark,hdfs_path,filedate,database_path,bookmark_path):
    
    #---Create database if not exist
    spark.sql("CREATE DATABASE IF NOT EXISTS itv000547_bikewise_raw_db")
    spark.sql("USE itv000547_bikewise_raw_db")
    
    table_name = spark.catalog.listTables('itv000547_bikewise_raw_db')
    
    table_name_flag = False
    
    for name in table_name:
        if list(name)[0] == "bikewise_incidents_table":
            table_name_flag = True
            break
            
    #---Read file from hdfs into dataframe
    bikewise_df = spark.read.json(f"{hdfs_path}/{filedate}.json")
    date_split=filedate.split('-')
    bikewise_df = bikewise_df. \
        withColumn('year', lit(date_split[0])). \
        withColumn('month',lit(date_split[1])). \
        withColumn('day', lit(date_split[2]))
    
    
    #---Write file into raw database
    if table_name_flag:
        bikewise_df. \
            write. \
            mode('append'). \
            partitionBy('year', 'month', 'day'). \
            parquet(f'{database_path}/itv000547_bikewise_raw_db.db/bikewise_incidents_table')
        
        spark.sql(f'''MSCK REPAIR TABLE bikewise_incidents_table''')
    else:
        bikewise_df. \
            write. \
            partitionBy('year', 'month', 'day'). \
            saveAsTable('bikewise_incidents_table')
        
    #---Updating Bookmark
    current_date_temp = datetime.datetime.strptime(filedate, "%Y-%M-%d")
    new_date = current_date_temp + datetime.timedelta(days=1)
    new_file_date = str(new_date).split(" ")[0] 
    update_bookmark_date(bookmark_path,new_file_date)
    