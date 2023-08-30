from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from datetime import timedelta
from airflow.utils.dates import days_ago


default_args = {

    'owner': 'manuel',
    'start_date': days_ago(0),
    'email': ['emmanoyelola@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),

}

# Define the DAG
dag = DAG(
    dag_id = 'ETL_toll_data',
    default_args = default_args,
    schedule_interval=timedelta(days = 1),
    description = 'Apache Airflow Final Assignment'
)


extract_transform_load = BashOperator(
task_id = 'extract_transform_load',
bash_command = '/home/project/airflow/dags/extract_transform_data.sh',
dag = dag
)

extract_transform_load