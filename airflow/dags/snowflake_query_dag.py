from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

# Constants for database and schema
DATABASE_ID = 'AIRFLOW'
SCHEMA_ID = 'SILVER'

# Define default arguments
default_args = {
    'owner': 'airflow',  # Owner of the DAG
    'start_date': days_ago(1),  # Start date for the DAG
    'retries': 3,  # Number of retries in case of failure
}

# Define the DAG
with DAG(
    dag_id='snowflake_incremental_load',  # Unique ID for the DAG
    default_args=default_args,  # Default arguments for the DAG
    schedule_interval='@daily',  # Run the DAG daily or cron like "*/2 * * * *" -> Run every 2 minutes etc
    catchup=False,  # Disable catchup to avoid backfilling
) as dag:

    # DummyOperator to mark the start of the workflow
    start = DummyOperator(task_id='start')

    # DummyOperator to mark the end of the workflow
    end = DummyOperator(task_id='end')

    # Task to execute the stored procedure for incremental load
    incremental_load = SnowflakeOperator(
        task_id='incremental_load',  # Unique ID for the task
        sql=f'CALL {DATABASE_ID}.{SCHEMA_ID}.INCREMENTAL_LOAD();',  # Call the stored procedure
        snowflake_conn_id='snowflake_conn',  # Snowflake connection ID defined in Airflow
    )

    # Set task dependencies
    start >> incremental_load >> end