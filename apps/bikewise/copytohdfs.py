import subprocess
from util import *

def copy_from_local(dir_path,filedate,bookmark_path,hdfs_path):
    
    
    #---Copy data to hdfs
    subprocess.check_output(f'hdfs dfs -mkdir -p {hdfs_path}', shell=True)
    subprocess.check_output(f'hdfs dfs -put {dir_path}/{filedate}.json {hdfs_path}',
                            shell=True)
    
    
    #---Update status in bookmark
    filestatus = "Copied"
    update_bookmark(bookmark_path,filedate,filestatus)