from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone
from airflow.hooks.base import BaseHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.mysql.operators.mysql import MySqlOperator
import duckdb

from dags.ingest.utils import check_table_exists


@dag(
    dag_id='customer',
    start_date=timezone.datetime(2024, 7, 5),
    params={'id': 'customer'},
    schedule_interval=None,
    catchup=False,
    template_searchpath="/opt/airflow/plugins",
    default_args={"owner": "korawica"},
)
def customer_dag():

    @task
    def hook_mysql():
        mysql_hook = MySqlHook(mysql_conn_id='warehouse', schema='warehouse')
        print(f"Test Connection to MySQL: {mysql_hook.test_connection()}")

    @task.branch(task_id="branching")
    def switch():
        table_status = check_table_exists(name='tnx_customer')
        print(f"Table Status: {table_status}")
        if table_status[0][0] == 0:
            print("Table 'customer_transaction' does not exists, creating it.")
            return 'create_table_task'
        print("Table 'tnx_customer' already exists.")
        return 'do_nothing'

    create_table_op = MySqlOperator(
        mysql_conn_id="warehouse",
        task_id="create_table_task",
        sql="sql/tnx_customer.sql",
    )

    do_nothing_op = EmptyOperator(task_id='do_nothing')

    @task(trigger_rule='one_success')
    def load_task():
        context = get_current_context()
        print(context["var"])

        execution_date: datetime = context["execution_date"]
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
    (
        hook_mysql.as_setup()
        >> switch()
        >> [create_table_op, do_nothing_op]
        >> load_task()
    )


customer_dag()
