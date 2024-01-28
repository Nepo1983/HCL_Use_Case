from datetime import datetime
from airflow import DAG
from typing import List, Optional
from airflow.decorators import dag, task
from pathlib import Path

import pandas as pd
import json
import logging

PARENT_DIR = Path(__file__).parent.resolve().parent
DATA_DIR = PARENT_DIR / 'data'
RAW_DATA_DIR = PARENT_DIR / 'data' / 'raw'
TRANSFORMED_DATA_DIR = PARENT_DIR / 'data' / 'transformed'

file_path_students = RAW_DATA_DIR / 'students.json'
file_path_missed = RAW_DATA_DIR / 'missed_days.json'

# Airflow DAG definition
@dag(schedule_interval=None, start_date=datetime(2024, 1, 1), catchup=False)
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
    def merge_data(students_data:pd.DataFrame, missed_days_data:pd.DataFrame) -> pd.DataFrame:
        # Merge data using 'student_id' as the key
        merged_data = pd.merge(students_data, missed_days_data, on='student_id', how='left')
        return merged_data
    
    @task
    def generate_csv(df:pd.DataFrame) -> None:
        # Generate csv file
        
        csv_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        
        csv_filename = f"{TRANSFORMED_DATA_DIR}/students_inf_{csv_timestamp}.csv"
                        
        df.to_csv(csv_filename, index=False)
    
    
    load_json_students = load_json_students(file_path_students)
    load_json_missed_days = load_json_missed_days(file_path_missed)
    merge_data = merge_data(load_json_students, load_json_missed_days)
    generate_csv = generate_csv(merge_data)