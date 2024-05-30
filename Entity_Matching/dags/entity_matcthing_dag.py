from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import os

from entity_engine import combine_layouts
from uid_engine import string_to_digits

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'entity_matching_dag',
    default_args=default_args,
    description='DAG to perform entity matching',
    schedule_interval='@once',
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Define the task to load data

def load_df_from_dir(dir_path):
    csv_files = [f for f in os.listdir(dir_path) if f.endswith(".csv")]
    layouts = []

    for csv_file in csv_files:
        file_path = os.path.join(dir_path, csv_file)
        df = pd.read_csv(file_path)
        # df['source'] = os.path.splitext(csv_file)[0]
        layouts.append(df)

    return layouts

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load_df_from_dir,
    op_kwargs={'dir_path': '/opt/airflow/data'},
    dag=dag,
)

# Define the task to preprocess data
def preprocess(ti):
    layouts = ti.xcom_pull(task_ids='load_task')

    for layout in layouts:
        layout['last_modified_date'] = datetime.now()

    def sanitize(df):
        return df.map(lambda x: x.replace(',', '').replace(' ', '').strip() if isinstance(x, str) else '' if pd.isna(x) else x)
    def create_soup(df, df_, soup, soup_name):
        df[soup_name] = df_[soup].apply(lambda x: ' '.join(x.values.astype(str)).lower(), axis=1)

    layout_copies = [layout.copy() for layout in layouts]
    soup = ['Name', 'Date of Birth', 'Father_Name']

    for layout, layout_copy, in zip(layouts, layout_copies):
        layout_copy = sanitize(layout_copy)
        create_soup(layout, layout_copy, soup, "soup")

    return layouts

preprocess_task = PythonOperator(
    task_id='preprocess_task',
    python_callable=preprocess,
    dag=dag,
)

# Define the task to save the result
def save_layouts(ti):
    layouts = ti.xcom_pull(task_ids='preprocess_task')
    final_df = layouts[0]

    for df in layouts[1:]:
        final_df = combine_layouts(final_df, df)

    final_df['uid'] = final_df['soup'].apply(
        string_to_digits)
    final_df.to_csv('/opt/airflow/result/final_result.csv', index=False)
    return final_df

save_task = PythonOperator(
    task_id='save_task',
    python_callable=save_layouts,
    dag=dag,
)

# Set task dependencies
load_task >> preprocess_task >> save_task
