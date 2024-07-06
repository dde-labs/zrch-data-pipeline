import logging

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import DagRun
from airflow.utils import timezone
from airflow.sensors.filesystem import FileSensor
from airflow.providers.mysql.hooks.mysql import MySqlHook


@dag(
    dag_id='load_customer',
    start_date=timezone.datetime(2024, 7, 5),
    params={'id': 'customer'},
    schedule_interval=None,
    catchup=False,
    default_args={"owner": "korawica"},
)
def load_customer_dag():
    """Loading customer Data that landing with JSON format to MySQL Database"""

    customer_sensor = FileSensor(
        task_id="is_file_available",
        fs_conn_id="file_local",
        filepath="raw/customer_transactions.json",
        poke_interval=5,
        timeout=20,
    )

    @task
    def hook_mysql(value, **context):
        print(value)
        logging.info(value)
        print(context)
        hook = MySqlHook(mysql_conn_id='warehouse')
        print(f"Test Connection to MySQL: {hook.test_connection()}")

    @task
    def end_task():
        context = get_current_context()
        print(f"Context: {context}")
        dag_run: DagRun = context["dag_run"]
        print(dag_run.conf.get("id"))

    customer_sensor >> hook_mysql("{{ params.id }}") >> end_task()


load_customer_dag()
