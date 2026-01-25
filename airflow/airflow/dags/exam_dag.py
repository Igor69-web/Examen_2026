from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='exam_dbt_pipeline',  # ← НОВОЕ имя!
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['exam', 'dbt'],
) as dag:
    
    run_task = BashOperator(
        task_id='dbt_run',
        bash_command='echo "Running dbt models"',
    )
    
    test_task = BashOperator(
        task_id='dbt_test',
        bash_command='echo "Testing dbt models"',
    )
    
    run_task >> test_task
