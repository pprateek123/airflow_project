import findspark
findspark.init()
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from pyspark.sql import SparkSession

dag = DAG(
    dag_id="read_write",
    start_date=datetime(2023, 1, 1),
    schedule_interval=" 0 0 * * *",
    catchup=False,

)

read_postgres_task = PostgresOperator(
    task_id='read_from_source_postgres',
    sql='SELECT * FROM source',
    postgres_conn_id='my_postgres',  
    dag=dag,
)


sql_query = """
 INSERT INTO destination
    (order_date, order_id, product, product_ean, category, purchase_address, quantity_ordered, price_each, cost_price, turnover, margin)
    VALUES
    ('2019-01-22 21:25:00', 141234, 'iPhone', 5638008983335000000, 'Vêtements', '944 Walnut St, Boston, MA 02215', 1, 700.0, 231.0, 700.0, 469.0),
    ('2019-01-28 14:15:00', 141235, 'Lightning Charging Cable', 5563319511488000000, 'Alimentation', '185 Maple St, Portland, OR 97035', 1, 14.95, 7.475, 14.95, 7.475);

"""

write_postgres_task = PostgresOperator(
    task_id='write_to_postgres',
    sql=sql_query,
    postgres_conn_id='write_postgres',  # Specify the connection ID
    autocommit=True,  # Set autocommit to True to commit the transaction immediately
    dag=dag,
)


# write_postgres_task = PostgresOperator(
#     task_id='write_to_destination_postgres',
#     sql='''
    # INSERT INTO destination
    # (order_date, order_id, product, product_ean, category, purchase_address, quantity_ordered, price_each, cost_price, turnover, margin)
    # VALUES
    # ('2019-01-22 21:25:00', 141234, 'iPhone', 5638008983335000000, 'Vêtements', '944 Walnut St, Boston, MA 02215', 1, 700.0, 231.0, 700.0, 469.0),
    # ('2019-01-28 14:15:00', 141235, 'Lightning Charging Cable', 5563319511488000000, 'Alimentation', '185 Maple St, Portland, OR 97035', 1, 14.95, 7.475, 14.95, 7.475);
#     ''',
#     postgres_conn_id='my_postgres',
#     dag=dag,
# )

read_postgres_task >> write_postgres_task