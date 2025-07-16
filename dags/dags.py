from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'minhds',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id = 'ecommerce_data_warehouse',
    description = 'DAG to crawl Tiki data, save to SQLite, transform, and load to PostgreSQL',
    default_args=default_args,
    schedule_interval = '@daily',
    start_date = datetime(2025, 7, 14),
    catchup = False
)

collection_data = BashOperator(
    task_id = 'crawl_data_from_tiki',
    bash_command = 'python /var/tmp/src/python_scripts/solve_crawl.py',
    dag = dag
)

transform_data = BashOperator(
    task_id = 'transform_raw_data',
    bash_command = 'python /var/tmp/src/python_scripts/transform.py',
    dag = dag
)

generate_data = BashOperator(
    task_id = 'generate_data',
    bash_command = 'python /var/tmp/src/python_scripts/fake_data.py',
    dag = dag
)

write_data_to_database = BashOperator(
    task_id = 'write_data_to_database',
    bash_command = 'python /var/tmp/src/sql_scripts/save_to_postgres.py',
    dag = dag
)

collection_data >> transform_data >> generate_data >> write_data_to_database



