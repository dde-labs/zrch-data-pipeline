from airflow.providers.mysql.hooks.mysql import MySqlHook


def check_table_exists(name: str):
    query: str = (
        f'SELECT count(1) as row_numbers '
        f'FROM information_schema.tables '
        f'WHERE table_name = "{name}"'
    )
    mysql_hook = MySqlHook(mysql_conn_id='warehouse', schema='warehouse')
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    connection.close()
    return results
