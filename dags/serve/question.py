from airflow.decorators import dag, task
from airflow.utils import timezone
from airflow.hooks.base import BaseHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
import duckdb


@dag(
    dag_id='question',
    start_date=timezone.datetime(2024, 7, 5),
    params={'id': 'question'},
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
        """Question: What are the top 2 best-selling products?"""
        conn = get_duckdb_mysql_conn()
        conn.sql(
            """
            SELECT
                MST.product_id,
                COALESCE(SUM(quantity * TNX.price), 0.0) AS sales_volume
            FROM mysqldb.warehouse.mst_product AS MST
            LEFT JOIN mysqldb.warehouse.tnx_customer AS TNX
                ON MST.product_id = TNX.product_id
            GROUP BY MST.product_id
            ORDER BY sales_volume DESC
            LIMIT 2
            """
        )

    @task
    def second_question_task():
        """Question: What is the average order value per customer?"""
        conn = get_duckdb_mysql_conn()
        conn.sql(
            """
            SELECT
                customer_id,
                COALESCE(
                    SUM(quantity * price),
                    0.0
                ) / COUNT(transaction_id)       AS average_order
            FROM mysqldb.warehouse.tnx_customer
            GROUP BY customer_id
            """
        )

    @task
    def third_question_task():
        """Question: What is the total revenue generated per product category?
        """
        conn = get_duckdb_mysql_conn()
        conn.sql(
            """
            SELECT
                category,
                COALESCE(SUM(quantity * TNX.price), 0.0) AS revenue
            FROM mysqldb.warehouse.mst_product AS MST
            LEFT JOIN mysqldb.warehouse.tnx_customer AS TNX
                ON MST.product_id = TNX.product_id
            GROUP BY category
            """
        ).show()

    hook_mysql() >> [
        first_question_task(),
        second_question_task(),
        third_question_task(),
    ]


question_dag()
