from datetime import datetime, timedelta
from distutils.version import StrictVersion
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

from make_connection import check_connection
default_args = {
    "owner": "Kirti",
    "depends_on_past": False,
    "start_date": datetime(2022, 3, 14),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=5)

}
def my_func(**kwargs):
    context = kwargs
    print("the run id is")
    print(context['dag_run'].run_id)

# setting schedule_interval="0 6 * * *" is task4 of our Assignmet
dag = DAG("Assignment", default_args=default_args, schedule_interval=timedelta(1),
          template_searchpath=['/usr/local/airflow/sql_files'],
          catchup=False)
t1 = DummyOperator(task_id='Welcome', dag=dag)

t2 = PythonOperator(task_id='python_run_id',  python_callable=my_func, provide_context=True, dag=dag )
print(t1.owner) # Airflow
t3 = PostgresOperator(task_id='make_table',postgres_conn_id='postgres_conn', sql='create_table.sql', dag=dag)

t1 >> t2 >> t3

