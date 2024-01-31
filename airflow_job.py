from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Define the DAG
dag = DAG(
    'execute_python_files',
    description='Execute Python files in a specific order',
    schedule_interval=None,  # You can define the schedule interval as needed
    start_date=datetime(2024, 1, 31),
    catchup=False,
)


def execute_02_stage_to_source():
    exec(open('/snowpark-e2e/02-stage-to-source.py').read())


def execute_source_to_curated(country):
    filename = f'/snowpark-e2e/03-source-to-curated-{country}.py'
    exec(open(filename).read())


def execute_curated_to_consumption():
    exec(open('/snowpark-e2e/04-curated-to-consumption.py').read())


# Define PythonOperator tasks to execute the Python function with country parameter
execute_stage_to_source_task = PythonOperator(
    task_id='execute_02_stage_to_source',
    python_callable=execute_02_stage_to_source,
    dag=dag,
)


execute_source_to_curated_FR_task = PythonOperator(
    task_id='execute_source_to_curated_FR',
    python_callable=execute_source_to_curated,
    op_kwargs={'country': 'FR'},
    dag=dag,
)

execute_source_to_curated_IN_task = PythonOperator(
    task_id='execute_source_to_curated_IN',
    python_callable=execute_source_to_curated,
    op_kwargs={'country': 'IN'},
    dag=dag,
)

execute_source_to_curated_US_task = PythonOperator(
    task_id='execute_source_to_curated_US',
    python_callable=execute_source_to_curated,
    op_kwargs={'country': 'US'},
    dag=dag,
)

execute_curated_to_consumption_task = PythonOperator(
    task_id='execute_curated_to_consumption',
    python_callable=execute_curated_to_consumption,
    dag=dag,
)

# Set task dependencies
execute_stage_to_source_task >> execute_source_to_curated_FR_task >> execute_source_to_curated_IN_task >> execute_source_to_curated_US_task >> execute_curated_to_consumption_task
