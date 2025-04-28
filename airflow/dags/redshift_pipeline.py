"""
This DAG uses python faker library to generate fake order data and uploads it to S3 then eventually copies it over to Redshift table

"""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook  # Use S3Hook to interact with S3
from airflow.operators.dummy import DummyOperator
import pandas as pd
from faker import Faker
import random
from io import StringIO
from datetime import datetime

# Constants
S3_BUCKET = '<s3_bucket_name>'  # Replace with your S3 bucket name
REDSHIFT_SCHEMA = '<redshift_schema>'  # Redshift schema
REDSHIFT_STAGING_TABLE = '<redshift_staging_table>'  # Redshift staging table

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Initialize Faker
fake = Faker()

# Function to generate CDC order data
def generate_cdc_order_data(num_rows=500):
    data = []
    for i in range(num_rows):
        order = {
            'order_id': i + 1,  # Numerical order ID (starting from 1)
            'customer_id': random.randint(1, 1000),  # Numerical customer ID (between 1 and 1000)
            'order_date': fake.date_this_year(),
            'status': random.choice(['CREATED', 'SHIPPED', 'DELIVERED', 'CANCELLED']),
            'product_id': random.randint(1, 100),  # Numerical product ID (between 1 and 100)
            'quantity': random.randint(1, 5),
            'price': round(random.uniform(10.0, 500.0), 2),
            'total_amount': 0.0,  # We'll calculate this next
            'cdc_timestamp': datetime.now()  # Simulate CDC timestamp
        }
        order['total_amount'] = round(order['quantity'] * order['price'], 2)
        data.append(order)

    # Convert to DataFrame
    df = pd.DataFrame(data)
    return df

# Function to upload data to S3
def upload_to_s3(bucket_name, file_name, df):
    # Use the S3Hook to interact with S3 using the `aws_default` connection
    s3_hook = S3Hook(aws_conn_id='aws_default')
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=file_name,
        bucket_name=bucket_name,
        replace=True
    )
    print(f"Data uploaded to s3://{bucket_name}/{file_name}")

# Function to generate data and upload to S3
def generate_and_upload_to_s3():
    # Generate 20 rows of fake CDC order data
    df_cdc_order_data = generate_cdc_order_data(num_rows=20)

    # Define S3 bucket and file path
    file_name = f"incoming_data/orders/cdc_order_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    # Upload the generated data to S3
    upload_to_s3(S3_BUCKET, file_name, df_cdc_order_data)

    # Return the S3 file name for downstream tasks
    return file_name

# Define the DAG
with DAG(
    dag_id='extract_and_load_data_to_redshift',  # Unique ID for the DAG
    default_args=default_args,  # Default arguments for the DAG
    schedule_interval='@daily',  # Run the DAG daily or cron like "*/2 * * * *" -> Run every 2 minutes etc
    catchup=False,  # Disable catchup to avoid backfilling
) as dag:
    
    # DummyOperator to mark the start of the workflow
    start = DummyOperator(task_id='start')

    # DummyOperator to mark the end of the workflow
    end = DummyOperator(task_id='end')

    # Task 1: Generate data and upload to S3
    generate_data_task = PythonOperator(
        task_id='generate_and_upload_to_s3',
        python_callable=generate_and_upload_to_s3,  # Call the function to generate and upload data
        dag=dag,
    )

    # Task 2: Copy data from S3 to Redshift staging
    copy_to_redshift_task = S3ToRedshiftOperator(
        task_id='copy_to_redshift',
        schema=REDSHIFT_SCHEMA,
        table=REDSHIFT_STAGING_TABLE,
        s3_bucket=S3_BUCKET,
        s3_key="{{ task_instance.xcom_pull(task_ids='generate_and_upload_to_s3') }}",  # Get file name from previous task
        redshift_conn_id='redshift_conn',  # Airflow connection ID for Redshift
        aws_conn_id='aws_default',  # Use the `aws_default` connection for S3 access
        copy_options=[
            "CSV",  # File format
            "IGNOREHEADER 1",  # Ignore the first row (header)
        ],
        dag=dag,
    )

    # Define task dependencies
    (
        start 
        >> generate_data_task 
        >> copy_to_redshift_task
        >> end 
    )