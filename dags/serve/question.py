from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.utils import timezone
from airflow.hooks.base import BaseHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
import duckdb


@dag(
    dag_id='customer',
    start_date=timezone.datetime(2024, 7, 5),
    params={'id': 'customer'},
    schedule_interval=None,
    catchup=False,
    default_args={"owner": "korawica"},
)
def question_dag():

    @task
    def hook_mysql():
        mysql_hook = MySqlHook(mysql_conn_id='warehouse', schema='warehouse')
        print(f"Test Connection to MySQL: {mysql_hook.test_connection()}")

    def get_duckdb_mysql_conn():
        hook = BaseHook.get_connection('warehouse')
        conn = duckdb.connect()
        conn.sql("INSTALL mysql")
        conn.sql("LOAD mysql")
        conn.sql(
            f"ATTACH 'host={hook.host} "
            f"user={hook.login} "
            f"passwd={hook.password} "
            f"port={hook.port} "
            f"db={hook.schema}' "
            f"AS mysqldb (TYPE MYSQL); "
            f"USE mysqldb;"
        )
        return conn

    @task
    def first_question_task():
        context = get_current_context()
        print(context["var"])

        execution_date: datetime = context["execution_date"]

        conn = get_duckdb_mysql_conn()
        conn.sql(
            f"BEGIN TRANSACTION; "
            f"DELETE mysqldb.warehouse.tnx_customer "
            f"WHERE load_date = '{execution_date:%Y-%m-%d %H:%M:%S}' "
            f";"
            f"INSERT INTO mysqldb.warehouse.tnx_customer BY NAME ( "
            f"SELECT DISTINCT "
            f"  transaction_id, "
            f"  customer_id, "
            f"  product_id, "
            f"  quantity, "
            f"  price, "
            f"  timestamp, "
            f"  load_date "
            f"FROM mysqldb.warehouse.raw_customer_transaction "
            f"WHERE load_date = '{execution_date:%Y-%m-%d %H:%M:%S}' "
            f")"
            f"COMMIT; "
        )

    @task
    def second_question_task():
        context = get_current_context()
        print(context["var"])

        execution_date: datetime = context["execution_date"]

        conn = get_duckdb_mysql_conn()
        conn.sql(
            f"BEGIN TRANSACTION; "
            f"DELETE mysqldb.warehouse.tnx_customer "
            f"WHERE load_date = '{execution_date:%Y-%m-%d %H:%M:%S}' "
            f";"
            f"INSERT INTO mysqldb.warehouse.tnx_customer BY NAME ( "
            f"SELECT DISTINCT "
            f"  transaction_id, "
            f"  customer_id, "
            f"  product_id, "
            f"  quantity, "
            f"  price, "
            f"  timestamp, "
            f"  load_date "
            f"FROM mysqldb.warehouse.raw_customer_transaction "
            f"WHERE load_date = '{execution_date:%Y-%m-%d %H:%M:%S}' "
            f")"
            f"COMMIT; "
        )

    @task
    def third_question_task():
        context = get_current_context()
        print(context["var"])

        execution_date: datetime = context["execution_date"]

        conn = get_duckdb_mysql_conn()
        conn.sql(
            f"BEGIN TRANSACTION; "
            f"DELETE mysqldb.warehouse.tnx_customer "
            f"WHERE load_date = '{execution_date:%Y-%m-%d %H:%M:%S}' "
            f";"
            f"INSERT INTO mysqldb.warehouse.tnx_customer BY NAME ( "
            f"SELECT DISTINCT "
            f"  transaction_id, "
            f"  customer_id, "
            f"  product_id, "
            f"  quantity, "
            f"  price, "
            f"  timestamp, "
            f"  load_date "
            f"FROM mysqldb.warehouse.raw_customer_transaction "
            f"WHERE load_date = '{execution_date:%Y-%m-%d %H:%M:%S}' "
            f")"
            f"COMMIT; "
        )

    hook_mysql.as_setup() >> [
        first_question_task(),
        second_question_task(),
        third_question_task(),
    ]


question_dag()
