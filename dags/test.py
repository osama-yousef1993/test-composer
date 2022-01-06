from datetime import date, datetime, timedelta
from airflow import models
from airflow.operators import bash


def test():
    YESTERDAY = datetime.now() - timedelta(1)

    default_args = {
        'owner': 'Composer Example',
        'depends_on_past': False,
        'email': [' '],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'start_date': YESTERDAY,
    }
    with models.DAG(
        'composer_quickstart',
            'catchup=False',
            default_args=default_args,
            schedule_interval=timedelta(days=1)) as dag:
        print_dag_run_conf = bash.BashOperator(
            task_id='print_dag_run_conf', bash_command='echo {{ dag_run.id }}')
        print_dag_run_conf
        print(print_dag_run_conf)
