from pendulum.datetime import DateTime
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
    dag_id='product',
    start_date=timezone.datetime(2024, 7, 5),
    params={'id': 'product'},
    schedule_interval=None,
    catchup=False,
    template_searchpath="/opt/airflow/plugins",
    default_args={"owner": "korawica"},
)
def product_dag():

    @task
    def hook_mysql():
        mysql_hook = MySqlHook(mysql_conn_id='warehouse', schema='warehouse')
        print(f"Test Connection to MySQL: {mysql_hook.test_connection()}")

    @task.branch(task_id="branching")
    def switch():
        table_status = check_table_exists(name='mst_product')
        print(f"Table Status: {table_status}")
        if table_status[0][0] == 0:
            print("Table 'mst_product' does not exists, creating it.")
            return 'create_table_task'
        print("Table 'mst_product' already exists.")
        return 'do_nothing'

    create_table_op = MySqlOperator(
        mysql_conn_id="warehouse",
        task_id="create_table_task",
        sql="sql/mst_product.sql",
    )

    do_nothing_op = EmptyOperator(task_id='do_nothing')

    @task(trigger_rule='one_success')
    def load_task():
        context = get_current_context()
        print(context["var"])
        logical_date: DateTime = context["logical_date"]

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
            f"""
            SELECT
                DIST_SRC.product_id,
                DIST_SRC.product_name,
                category,
                CASE WHEN TNX_CUST.price > 0 then TNX_CUST.price
                    ELSE DIST_SRC.price
                    END AS price,
                '{logical_date:%Y-%m-%d}' AS load_date
            FROM (
                    SELECT DISTINCT 
                        product_id,
                        product_name,
                        category,
                        price
                    FROM mysqldb.warehouse.raw_product_catalog 
                    WHERE load_date = '{logical_date:%Y-%m-%d}'
                        AND price >= 0
                ) AS DIST_SRC
            LEFT JOIN (
                    SELECT
                        product_id,
                        MAX(price) AS price
                    FROM mysqldb.warehouse.tnx_customer
                    WHERE load_date = '{logical_date:%Y-%m-%d}'
                        AND price > 0
                    GROUP BY product_id
                ) AS TNX_CUST
                ON DIST_SRC.product_id = TNX_CUST.product_id
            """
        ).show()
        conn.sql(
            f"""USE mysqldb;
            BEGIN TRANSACTION;
            INSERT INTO mysqldb.warehouse.mst_product BY NAME ( 
            SELECT
                DIST_SRC.product_id,
                DIST_SRC.product_name,
                category,
                CASE WHEN TNX_CUST.price > 0 then TNX_CUST.price
                    ELSE DIST_SRC.price
                    END AS price,
                '{logical_date:%Y-%m-%d}' AS load_date
            FROM (
                    SELECT DISTINCT 
                        product_id,
                        product_name,
                        category,
                        price
                    FROM mysqldb.warehouse.raw_product_catalog 
                    WHERE load_date = '{logical_date:%Y-%m-%d}'
                        AND price >= 0
                ) AS DIST_SRC
            LEFT JOIN (
                    SELECT
                        product_id,
                        MAX(price) AS price
                    FROM mysqldb.warehouse.tnx_customer
                    WHERE load_date = '{logical_date:%Y-%m-%d}'
                        AND price > 0
                    GROUP BY product_id
                ) AS TNX_CUST
                ON DIST_SRC.product_id = TNX_CUST.product_id
            );
            COMMIT;
            """
        )

    (
        hook_mysql()
        >> switch()
        >> [create_table_op, do_nothing_op]
        >> load_task()
    )


product_dag()
