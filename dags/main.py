# main_dag.py
import csv
import random
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


def generate_dummy_data():
    # Generate random data for 100 rows
    data = {
        'id': list(range(1, 101)),
        'loan_amnt': [random.randint(1000, 10000) for _ in range(100)],
        'term': [random.choice(['36 months', '60 months']) for _ in range(100)],
        'interest_rate': [random.randint(5, 15) for _ in range(100)],
    }

    # Create a CSV file
    with open('dummy_data.csv', 'w', newline='') as csvfile:
        fieldnames = ['id', 'loan_amnt', 'term', 'interest_rate']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for i in range(100):
            writer.writerow({field: data[field][i] for field in fieldnames})

    print("Dummy data generated and saved to dummy_data.csv")

# Call function
generate_dummy_data()

from sqlalchemy import create_engine


# Set up database connection
postgres_conn_id = 'postgresql+psycopg2://postgres:postgres@localhost:5434/postgres'
engine = create_engine(postgres_conn_id)

# Functions for each step in the pipeline
def authenticate():
    print("Authentication step")

def assess_risk():
    print("Risk Assessment step")

def fetch_loan_data():
    # Adjust to fetch data for Data Scientist branch
    df = pd.read_csv('dummy_data.csv')
    df.to_sql('loan_data', engine, index=False, if_exists='replace')
    print("Fetch Loan Data step")

def data_engineer_handoff():
    print("Data Engineer Team: Data handoff to Data Scientist through DB")

def update_loan_data():
    # here will create a logic to pull date from the updated table in DB
    print("Update Loan Data step")

def data_scientist_handoff():
    # Mention the specific table or database for clarity
    print("Data Scientist DAG: Updated data handoff to PostgreSQL Database (e.g., table: loan_data)")

def generate_reports():
    print("Generate Reports step")

# Set up Airflow DAG
default_args = {
    'owner': 'rose',
    'start_date': datetime(2024, 1, 22),
    'retries': 1,
}

dag = DAG(
    'loan_processing_pipeline',
    default_args=default_args,
    schedule_interval="@daily",
)

generate_data_task = PythonOperator(
    task_id='dummy_upload_loan_data',
    python_callable=generate_dummy_data,
    dag=dag,
)

auth_task = PythonOperator(
    task_id='authenticate',
    python_callable=authenticate,
    dag=dag,
)

risk_assessment_task = PythonOperator(
    task_id='EDA_assess_risk',
    python_callable=assess_risk,
    dag=dag,
)

fetch_loan_data_task = PythonOperator(
    task_id='fetch_loan_data',
    python_callable=fetch_loan_data,
    dag=dag,
)

data_engineer_handoff_task = PythonOperator(
    task_id='data_engineer_handoff',
    python_callable=data_engineer_handoff,
    dag=dag,
)

update_loan_data_task = PythonOperator(
    task_id='update_loan_data',
    python_callable=update_loan_data,
    dag=dag,
)

data_scientist_handoff_task = PythonOperator(
    task_id='data_scientist_handoff',
    python_callable=data_scientist_handoff,
    dag=dag,
)

generate_reports_task = PythonOperator(
    task_id='generate_reports',
    python_callable=generate_reports,
    dag=dag,
)



# Define task dependencies
auth_task >> generate_data_task >> risk_assessment_task

with dag:
    de_processing = data_engineer_handoff_task >> update_loan_data_task
    ds_processing = data_scientist_handoff_task >> fetch_loan_data_task

    de_processing >> generate_reports_task
    ds_processing >> de_processing


