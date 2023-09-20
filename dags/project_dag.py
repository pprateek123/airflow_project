import json 
import csv
from datetime import datetime
from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator  
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator


with DAG(
    dag_id = 'project_dag',
    schedule_interval = '*/5 * * * *',
    start_date = datetime(2023,9,15),
    catchup = False,
) as dag:
    
   
   
    is_api_active = HttpSensor(
        task_id = "is_api_active",
        http_conn_id = "http_airflow",
        endpoint = 'todos/',

    )

    task_get_todos = SimpleHttpOperator(
        task_id = 'get_todos',
        http_conn_id = 'http_airflow',
        endpoint = 'todos/',
        method = 'GET',
        response_filter = lambda response: json.loads(response.text) ,
        log_response = True,

    )
 
    def convert_json_to_csv(**kwargs):
        """
        converts json to csv 

        Args:
            **kwargs (dict): The context provided by Apache Airflow, which  contains information
            about the task execution environment. 

        """

        
        task_instance = kwargs['ti']
        response_data = task_instance.xcom_pull(task_ids="get_todos")  
        
        if response_data:
           
            csv_file_path = '/home/ubuntu/airflow/Airflow_assignments/todos.csv'  
            
            with open(csv_file_path, 'w', newline='') as csvfile:
                csv_writer = csv.writer(csvfile)
                
                header = list(response_data[0].keys())
                csv_writer.writerow(header)
                
                for item in response_data:
                    csv_writer.writerow(list(item.values()))
        else:
            print("No JSON data available in XCom to convert to CSV.")

    task_convert_to_csv = PythonOperator(
        task_id="convert_to_csv",
        python_callable=convert_json_to_csv,
        provide_context=True, 
    )


    file_sensor_task = FileSensor(
        task_id='file_sensor_task',
        filepath='/home/ubuntu/airflow/Airflow_assignments/todos.csv',
        poke_interval=15, 
        timeout=300,  
        mode='poke', 
        ) 

    move_files_task = BashOperator(
        task_id='move_files',
        bash_command='mv  /home/ubuntu/airflow/Airflow_assignments/todos.csv /tmp/',
    )


    
    load_to_postgres_task = PostgresOperator(
        task_id="load_to_postgres_task",
        postgres_conn_id="write_postgres",  
        sql="""TRUNCATE TABLE todos_table;
                COPY todos_table FROM '/tmp/todos.csv' CSV HEADER;""", 
    )


    spark_submit_bash = BashOperator(
        task_id='submit_spark',
        bash_command='/opt/spark/bin/spark-submit --driver-class-path /lib/jvm/java-11-openjdk-amd64/lib/postgresql-42.6.0.jar /home/ubuntu/airflow/python_scripts/script.py',
    )

    read_from_postgres = PostgresOperator(
        task_id="read_todos",
        postgres_conn_id="write_postgres",
        sql="SELECT * FROM todos_table_spark;",
    )

    def log_result(ti):
        """
        This function logs response to the console

        Args:
           ti (airflow.models.TaskInstance): The TaskInstance object representing the current task's execution context.
            This parameter is automatically passed by Airflow when the function is used as a PythonOperator.
        """
        result = ti.xcom_pull(task_ids='read_todos')
        for row in result:
         print(row) 
    
    log_result_task = PythonOperator(
        task_id = "log_result",
        python_callable = log_result,
        provide_context = True,
    )


#task flow 
is_api_active >> task_get_todos >> task_convert_to_csv >> file_sensor_task >> move_files_task >> load_to_postgres_task >> spark_submit_bash >> read_from_postgres >> log_result_task


