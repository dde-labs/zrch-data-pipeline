import logging

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.operators.empty import EmptyOperator
from airflow.models import DagRun
from airflow.utils import timezone
from airflow.hooks.base import BaseHook
from airflow.sensors.filesystem import FileSensor
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.mysql.operators.mysql import MySqlOperator
import duckdb

from dags.ingest.utils import check_table_exists


@dag(
    dag_id='load_product',
    start_date=timezone.datetime(2024, 7, 5),
    params={'id': 'product'},
    schedule_interval=None,
    catchup=False,
    template_searchpath="/opt/airflow/plugins",
    default_args={"owner": "korawica"},
)
def load_product_dag():
    """Loading Product Data that landing with CSV format to MySQL Database"""

    product_sensor = FileSensor(
        task_id="is_file_available",
        fs_conn_id="file_local",
        filepath="raw/product_catalog.csv",
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

    @task.branch(task_id="branching")
    def switch():
        table_status = check_table_exists(name='product_catalog')
        print(f"Table Status: {table_status}")
        if table_status[0][0] == 0:
            print("Table 'customer_transaction' does not exists, creating it.")
            return 'create_table_task'
        print("Table 'customer_transaction' already exists.")
        return 'do_nothing'

    create_table_op = MySqlOperator(
        mysql_conn_id="warehouse",
        task_id="create_table_task",
        sql="sql/product_catalog.sql",
    )

    do_nothing_op = EmptyOperator(task_id='do_nothing')

    @task(trigger_rule='one_success')
    def ingest_task():
        context = get_current_context()
        print(f"Context: {context}")
        dag_run: DagRun = context["dag_run"]
        print(dag_run.conf.get("id"))

        # NOTE: test duckdb
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
        # conn.sql("SHOW TABLES;").show()
        data_path: str = '/opt/airflow/data'
        conn.sql(
            f"SELECT "
            f" product_id, "
            f" product_name, "
            f" category, "
            f" CAST( "
            f"   CASE WHEN price = 'invalid_price' THEN '0.0' "
            f"        ELSE price "
            f"   END AS DOUBLE "
            f" ) AS price "
            f"FROM read_csv("
            f" '{data_path}/raw/product_catalog.csv', "
            f" delim = ',',  header = true "
            f") LIMIT 10 "
            f";"
        ).show()
        conn.sql(
            f"INSERT INTO mysqldb.warehouse.product_catalog BY NAME ( "
            f"SELECT "
            f" product_id, "
            f" product_name, "
            f" category, "
            f" CAST( "
            f"   CASE WHEN price = 'invalid_price' THEN '0.0' "
            f"        ELSE price "
            f"   END AS DOUBLE "
            f" ) AS price "
            f"FROM read_csv("
            f" '{data_path}/raw/product_catalog.csv', "
            f" delim = ',', header = true "
            f")"
            f");"
        )

    (
            product_sensor
            >> hook_mysql("{{ params.id }}")
            >> switch()
            >> [create_table_op, do_nothing_op]
            >> ingest_task()
    )


load_product_dag()
