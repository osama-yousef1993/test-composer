import datetime as dt
import os

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator

target_type = 'attributed-events'
target_bucket = Variable.get("MediaMath")
target_directory = target_bucket + target_type + '/' + \
    '{{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y/%m/%d") }}'

DAGS_FOLDER = os.environ["DAGS_FOLDER"]
bash_exec = f"{DAGS_FOLDER}/scripts/batch.sh " + ' -d ' + \
    target_directory + ' -t ' + target_type + ' -c'

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2021, 11, 16, 7, 0, 0),
    'retries': 3,
}

with DAG('at_datalake_staging' + target_type,
         default_args=default_args,
         schedule_interval='@daily',
         catchup=True
         ) as dag:

    log_exec_params = BashOperator(
        task_id='exec_parameters', bash_command='echo ' + bash_exec)
    exec_batch_script = BashOperator(
        task_id='exec_batch_script', bash_command='bash ' + bash_exec)

    log_exec_params >> exec_batch_script
