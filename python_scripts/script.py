import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("sparkfunction")\
        .config("spark.jars", "/usr/lib/jvm/java-11-openjdk-amd64/lib/postgresql-42.6.0.jar").getOrCreate()

        # user_df = spark.read.format("csv").option("url", "jdbc:postgresql://localhost:5432/postgres") \
        # .option("driver", "org.postgresql.Driver").option("dbtable", "todos_table") \
        # .option("user", 'postgres').option("password", 'postgres').load()


df=spark.read.format("csv").option("header","true").load("/tmp/todos.csv")



db_url = "jdbc:postgresql://localhost:5432/postgres"
table_name = "todos_table_spark"
properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

## selecting only odd ids 
df = df.filter(col('id') % 2 == 1)

df.write.parquet("/home/ubuntu/airflow/Airflow_output/todos.parquet", mode="overwrite")


df.write.mode("overwrite").jdbc(url=db_url, table=table_name, properties=properties)


df.show()
spark.stop()