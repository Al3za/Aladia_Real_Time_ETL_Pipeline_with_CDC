# flusso corretto con Spark + Kafka (CDC)

from pyspark.sql import SparkSession
# Creare Spark session con supporto Kafka:
spark = SparkSession.builder \
    .appName("EmployeeCDC_ETL") \
    .getOrCreate()

# Leggere dal topic di kafka. ricorda che i topik sono i messaggi CDC salvati qui in kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "cdc.public.Employee") \
    .option("startingOffsets", "earliest") \
    .load()

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
    .start()

query.awaitTermination()
# In questo modo vedi i messaggi CDC in tempo reale, man mano che vengono generati da Debezium