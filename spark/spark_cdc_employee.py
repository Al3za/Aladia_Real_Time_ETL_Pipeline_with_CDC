import os
# Creare Spark session con supporto Kafka:
from pyspark.sql import SparkSession

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JARS_DIR = os.path.join(BASE_DIR, "jars")
print(JARS_DIR, 'JARS_DIR here')

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CDC_Employee")
    .master("local[*]")
    .config(
        "spark.jars",
        "/home/ale_linux/Aladia_Real_Time_ETL_Pipeline_with_CDC/jars/spark-sql-kafka-0-10_2.12-3.4.1.jar,"
        "/home/ale_linux/Aladia_Real_Time_ETL_Pipeline_with_CDC/jars/kafka-clients-3.4.1.jar,"
        "/home/ale_linux/Aladia_Real_Time_ETL_Pipeline_with_CDC/jars/spark-token-provider-kafka-0-10_2.12-3.4.1.jar,"
        "/home/ale_linux/Aladia_Real_Time_ETL_Pipeline_with_CDC/jars/commons-pool2-2.11.1.jar,"
        "/home/ale_linux/Aladia_Real_Time_ETL_Pipeline_with_CDC/jars/metrics-core-2.2.0.jar"
    )
    # .config("spark.sql.streaming.checkpointLocation", "/tmp/spark/checkpoint")
    .getOrCreate()
) 


spark.sparkContext.setLogLevel("WARN")
print("Spark ready! Version:", spark.version)


# Leggere dal topic di kafka. ricorda che i topik sono i messaggi CDC salvati qui in kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("subscribe", "cdc.public.Employee") \
    .option("startingOffsets", "earliest") \
    .load()
#    .option("group.id", "test_spark_group_01") \
# cdc.public.Employee = il prefisso con la quale estrapoliamo i topic(message) di kafka dopo ogni CRUD operation

# il port del nostro service kafka (quando abbiamo startato compose)
# cdc.public.Employee, coordinate del nostro registro 

# Trasformare i dati (estrarre payload.after,(i nuovi dati stored in kafka e definiti nel "payload"
# response dopo che facciamomun INSERT al db)):
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, LongType

# Estarai solo i dati che ci servono da payload
schema = StructType([
    StructField("payload", StructType([ # res #payload" (nel message kafka)
        StructField("after", StructType([ # extraxt "after":values(the latest insert, stored in "payload")
            StructField("id", LongType()),
            StructField("email", StringType()),
            StructField("name", StringType()),
            StructField("createdAt", LongType()),
            StructField("updatedAt", LongType()),
            StructField("role", StringType()),
            StructField("password", StringType())
        ]))
    ]))
])

# parse the "payload" message data stored in kafka.
# serve parsare solo payload.after (non flat JSON)
df_parsed = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json("json_str", schema).alias("data")) \
    .select("data.payload.after.*")


# Scrivere in output (console o DB):

query = df_parsed.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp_spark/checkpoint") \
    .start()


query.awaitTermination()
# In questo modo vedi i messaggi CDC in tempo reale nella console(.format("console")), man mano che vengono generati da Debezium, cosi:
# Batch: 1
# -------------------------------------------
# +---+-----------------+--------+-------------+-------------+-----+--------------------+
# | id|            email|    name|    createdAt|    updatedAt| role|            password|
# +---+-----------------+--------+-------------+-------------+-----+--------------------+
# |100|NewUser3@mail.com|NewUser3|1769372701314|1769372701314|ADMIN|$2b$10$lWWKmGVGdt...|
# +---+-----------------+--------+-------------+-------------+-----+--------------------+

# -------------------------------------------                                     
# Batch: 2
# -------------------------------------------
# +---+-----------------+--------+-------------+-------------+-----+--------------------+
# | id|            email|    name|    createdAt|    updatedAt| role|            password|
# +---+-----------------+--------+-------------+-------------+-----+--------------------+
# |101|NewUser4@mail.com|NewUser4|1769372758974|1769372758974|ADMIN|$2b$10$/780qI4KUM...|
# +---+-----------------+--------+-------------+-------------+-----+--------------------+


# grazie  a .option("checkpointLocation", "/tmp_spark/checkpoint") \, spark sa quali di questi dati ha gia visto, e fare

#  mkdir -p /tmp_spark/checkpoint


# C:\Users\ale\OneDrive\Dokument\Downloads\commons-pool2-2.11.1.jar