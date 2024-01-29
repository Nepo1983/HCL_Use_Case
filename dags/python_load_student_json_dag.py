#%%
from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
from io import StringIO
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator

import sqlalchemy
import pandas as pd
import json
import logging
import boto3

#%%
default_args = {
    "owner":"airflow",
    "depends_on_past":False,
}

# Airflow DAG definition
@dag(
    "load_json_dag",
    default_args=default_args,
    description="HCL Final Coding Evaluation",
    catchup=False, 
    schedule=None,
    start_date=datetime(2023, 11, 13),
    tags=["uma-tag-qualquer"],
)

def load_json_dag():    
    @task
    def load_json_students(file_path):
                       
        # Configure logging
        logging.basicConfig(filename='load_process.log', level=logging.INFO)
    
        try:
            # Load JSON data from file
            with open(file_path, 'r') as file:
                data = json.load(file)

            # Check if the 'students' key exists in the JSON data
            if 'students' not in data:
                logging.error("Error: 'students' key not found in JSON file.")
                return None

            # Convert JSON data to DataFrame
            df = pd.json_normalize(data['students'])

            # Validate DataFrame structure
            required_columns = ['student_id', 'name', 'grades.math', 'grades.science', 'grades.history', 'grades.english']
            if not all(column in df.columns for column in required_columns):
                logging.error("Error: Incorrect structure in DataFrame columns.")
                return None

            # Log successful load
            logging.info("JSON file loaded successfully.")

            dict_nm_columns = {'grades.math':'grades_math', 'grades.science':'grades_science', 'grades.history':'grades_history', 'grades.english':'grades_english'}
            df.rename(columns=dict_nm_columns, inplace=True) 

            return df

        except Exception as e:
            logging.error(f"Error during JSON file loading: {str(e)}")
            return None
    
    @task()
    def load_json_missed_days(file_path):
        
        # Configure logging
        logging.basicConfig(filename='load_process.log', level=logging.INFO)

        try:
            # Load JSON data from file
            with open(file_path, 'r') as file:
                data = json.load(file)

            # Check if the 'missed_classes' key exists in the JSON data
            if 'missed_classes' not in data:
                logging.error("Error: 'missed_classes' key not found in JSON file.")
                return None

            # Convert JSON data to DataFrame
            df = pd.json_normalize(data['missed_classes'])

            # Validate DataFrame structure
            required_columns = ['student_id', 'missed_days']
            if not all(column in df.columns for column in required_columns):
                logging.error("Error: Incorrect structure in DataFrame columns.")
                return None

            # Log successful load
            logging.info("JSON file loaded successfully.")

            return df

        except Exception as e:
            logging.error(f"Error during JSON file loading: {str(e)}")
            return None
    
    @task()
    def merge_data(students_data, missed_days_data):
        """
        Generate merged dataframes containing student and missed days data

        Args:
            students_data (_type_): _description_
            missed_days_data (_type_): _description_

        Returns:
            pd.DataFrame: Merged dataframe
        """
        # Merge data using 'student_id' as the key
        merged_data = pd.merge(students_data, missed_days_data, on='student_id', how='left')
        return merged_data
    
    @task
    def generate_csv(df) -> None:
        """
        Generates a CSV file with merged dataframe

        Args:
            df (_type_): _description_

        Returns:
            str: name of generated CSV
        """
        
        csv_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        
        csv_filename = f"dags/data/transformed/students_inf_{csv_timestamp}.csv"
                        
        df.to_csv(csv_filename, index=False)
        
        return f"dags/data/transformed/students_inf_{csv_timestamp}.csv"
    
    @task
    def upload_csv_to_s3(file_path, bucket_name, object_key, aws_access_key_id=None, aws_secret_access_key=None):
        
        """
        Uploads a CSV file to an AWS S3 bucket.

        Parameters:
        - file_path (str): Local path to the CSV file.
        - bucket_name (str): Name of the S3 bucket.
        - object_key (str): Object key (path) in the S3 bucket.
        - aws_access_key_id (str, optional): AWS access key ID.
        - aws_secret_access_key (str, optional): AWS secret access key.

        Returns:
        - bool: True if the upload was successful, False otherwise.
        """
        try:
            # Create an S3 client
            s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id,    aws_secret_access_key=aws_secret_access_key)

            # Upload the file
            s3.upload_file(file_path, bucket_name, object_key)

            print(f"File '{file_path}' uploaded to S3 bucket '{bucket_name}' with object    key '{object_key}'.")
            return True

        except Exception as e:
            print(f"Error uploading file to S3: {str(e)}")
            return False

# Replace the placeholders below with your AWS credentials (if not using IAM roles)
    csv_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    file_path = f"dags/data/transformed/students_inf_{csv_timestamp}.csv"
    bucket_name = 'snowflake-series'
    object_key = f's3://snowflake-series/tmp/students_inf_{csv_timestamp}.csv'
    aws_access_key_id = 'AKIA2SRWH43TIS5PQXUB'
    aws_secret_access_key = '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'
    
    file_path_students = 'dags/data/raw/students.json'
    file_path_missed = 'dags/data/raw/missed_days.json'
    csv_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    csv_filename = f"dags/data/transformed/students_inf_{csv_timestamp}.csv"
    
    students_data = load_json_students(file_path_students)
    missed_days_data = load_json_missed_days(file_path_missed)
    merged_data = merge_data(students_data, missed_days_data)
    generate_csv = generate_csv(merged_data)
    upload_csv_to_s3 = upload_csv_to_s3(file_path, bucket_name, object_key, aws_access_key_id, aws_secret_access_key)
    generate_csv >> upload_csv_to_s3
    
dag_load_json_instance = load_json_dag()