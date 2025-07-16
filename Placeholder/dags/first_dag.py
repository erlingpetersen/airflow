import os
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json

# Function to fetch data from JSONPlaceholder and save to file
def fetch_and_save_posts(**context):
    url = 'https://jsonplaceholder.typicode.com/posts'
    response = requests.get(url)
    response.raise_for_status()
    posts = response.json()
    output_dir = os.path.join(os.path.dirname(__file__), '..', 'output')
    output_path = os.path.abspath(os.path.join(output_dir, 'jsonplaceholder_posts.json'))
    os.makedirs(output_dir, exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(posts, f, indent=2)

with DAG(
    dag_id='jsonplaceholder_fetch_posts',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    description='Fetch posts from JSONPlaceholder and save to file',
) as dag:
    fetch_posts = PythonOperator(
        task_id='fetch_and_save_posts',
        python_callable=fetch_and_save_posts,
        provide_context=True,
    ) 