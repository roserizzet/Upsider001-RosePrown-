# import_data_dag.py... this is incase of real data..!

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import psycopg2
from psycopg2 import sql

# CSV file path
csv_file_path = "path/to/your/data.csv"

# PostgreSQL database connection parameters
db_params = {
    "host": "localhost",
    "port": 5434,
    "database": "Postgres",
    "user": "postgres",
    "password": "postgres",
}

def import_data():
    # Create a connection to the PostgreSQL database
    conn = psycopg2.connect(**db_params)

    # Create a cursor
    cursor = conn.cursor()

    # Read data from the CSV file using pandas
    data = pd.read_csv(csv_file_path)

    # Define the PostgreSQL table and columns
    table_name = "p2prisks"
    columns = ["borrowerid", "loanamt", "term", "interestrate"]  # 

    # Create the table if it doesn't exist
    create_table_query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {} (
            id SERIAL PRIMARY KEY,
            {} VARCHAR(255),
            {} VARCHAR(255),
            {} VARCHAR(255),
            {} VARCHAR(255)
            -- Add more columns 
        )
    """).format(
        sql.Identifier(table_name),
        sql.Identifier(columns[0]),
        sql.Identifier(columns[1]),
        sql.Identifier(columns[2]),
        sql.Identifier(columns[3])
        # 
    )

    cursor.execute(create_table_query)
    conn.commit()

    # Insert data into the PostgreSQL table
    insert_query = sql.SQL("""
        INSERT INTO {}
        ({})
        VALUES
        (%s, %s, %s, %s)
    """).format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(map(sql.Identifier, columns)),
    )

    for index, record in data.iterrows():
        # Assuming data is a DataFrame where columns correspond to column names
        values = [record.get(col, None) for col in columns]
        cursor.execute(insert_query, values)

    conn.commit()

    # Close the cursor and connection
    cursor.close()
    conn.close()

# Airflow DAG definition
default_args = {
    'owner': 'rose2',
    'start_date': datetime(2024, 1, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'import_data_dag',
    default_args=default_args,
    schedule_interval='@daily',
)

import_data_task = PythonOperator(
    task_id='import_data_task',
    python_callable=import_data,
    dag=dag,
)

import_data_task
