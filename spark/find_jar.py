import os

# jar_path_kafka = "C:/Users/ale/etl_cdc_project/jars/kafka-clients-3.4.1.jar"
jar_path_spark_sql = "C:/Users/ale/etl_cdc_project/jars/spark-sql-kafka-0-10_2.12-3.4.1.jar"

print(os.path.exists(jar_path_spark_sql), 'jar_path_spark_sql')  # deve stampare True
# print(os.path.exists(jar_path_kafka), 'jar_path_kafka')  # deve stampare True
