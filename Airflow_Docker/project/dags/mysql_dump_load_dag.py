from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.mysql_operator import MySqlOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'mysql_table_dump',
    default_args=default_args,
    description='A DAG to dump a MySQL table',
    schedule_interval='@once',
    catchup=False,
)

dump_command = ("mysqldump -u root -pmysql@123 -h host.docker.internal -P 3306 customer customer_profile > "
                "/opt/airflow/dags/dump_file.sql 2>/opt/airflow/dags/dump_error.log")

dump_task = BashOperator(
    task_id='mysql_table_dump_task',
    bash_command=dump_command,
    dag=dag
)

load_command = "mysql -u root -pmysql@123 -h host.docker.internal -P 3306 mydb < /opt/airflow/dags/dump_file.sql"

load_task = BashOperator(
    task_id='mysql_table_load_task',
    bash_command=load_command,
    dag=dag
)

dump_task >> load_task

# mysql_query = """
#     CREATE TABLE free(
#         name varchar(255)
#     );
# """
#
# mysql_task = MySqlOperator(
#     task_id='mysql_task',
#     mysql_conn_id='MySQLID',  # Connection ID as configured in Airflow
#     sql=mysql_query,
#     dag=dag,
# )

