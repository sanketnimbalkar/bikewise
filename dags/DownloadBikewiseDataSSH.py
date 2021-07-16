from datetime import timedelta

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.utils.dates import days_ago
from airflow.models import Variable


args = {
    'owner': 'airflow',
}

sshHook = SSHHook(ssh_conn_id='ITVersity Gateway')

with DAG(
    dag_id='download_bikewise_data_ssh',
    default_args=args,
    schedule_interval='0 0 * * *',
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=60),
) as dag:

    # [START airflow_dag]
    #---File ingestion
    python_loc = Variable.get('ITV_PYTHON_VENV_DIR')
    apps_dir = Variable.get('ITV_APPS_DIR')
    
    download_file = SSHOperator(
        task_id='download_file',
        command=f'{python_loc}/python {apps_dir}/bikewise/app.py lab {apps_dir}/bikewise/conf.yml',
        ssh_hook=sshHook,
    )
    
    #---Copy to hdfs
    copy_hdfs = SSHOperator(
        task_id='copy_hdfs',
        command=f'{python_loc}/python {apps_dir}/bikewise/hdfs.py',
        ssh_hook=sshHook,
    )
    
    #---Storing to database
    store_database = SSHOperator(
        task_id='store_database',
        command=f'{python_loc}/python {apps_dir}/bikewise/sparkmain.py',
        ssh_hook=sshHook,
    )
    # [END airflow_dag]

download_file >> copy_hdfs >> store_database

if __name__ == "__main__":
    dag.cli()