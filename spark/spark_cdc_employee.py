# RICORDA DI STARTARE i container e registrare il "connector" prima di startare questo file

# flusso corretto con Spark + Kafka (CDC)
import os
# Creare Spark session con supporto Kafka:
from pyspark.sql import SparkSession

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JARS_DIR = os.path.join(BASE_DIR, "jars")
print(JARS_DIR, 'JARS_DIR here')

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CDC_Employee") \
    .master("local[*]") \
    .config("spark.jars",
        "file:///C:/Users/ale/etl_cdc_project/jars/spark-sql-kafka-0-10_2.12-3.4.1.jar,"
        "file:///C:/Users/ale/etl_cdc_project/jars/kafka-clients-3.4.1.jar,"
        "file:///C:/Users/ale/etl_cdc_project/jars/spark-token-provider-kafka-0-10_2.12-3.4.1.jar") \
    .config("spark.hadoop.io.native.lib.available", "false") \
    .config("spark.hadoop.disable.native.lib", "true") \
    .config("spark.sql.streaming.checkpointLocation", "C:/tmp_spark/checkpoint") \
    .getOrCreate()
# Spark Structured Streaming non funzionante a causa di problemi con winutils.exe e windowsIo.files crash
 # .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \ spark not working. try to add it to see if it works
    # .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
spark.sparkContext.setLogLevel("WARN")
print("Spark ready! Version:", spark.version)


# spark = SparkSession.builder \
#     .appName("EmployeeCDC_ETL") \
#     .config( # connettore Spark ↔ Kafka, (un jar Java) che permette a queste dui di creare la pipeline(CDC/ETL) grazie ai jars/file definiti sotto. Senza questa confi: "Failed to find data source: kafka" 
#        "spark.jars",
#        f"{BASE_DIR}/jars/spark-sql-kafka-0-10_2.12-3.4.1.jar," #  (file java che servono per comunicqazione tra kafka e spark)
#        f"{BASE_DIR}/jars/kafka-clients-3.3.2.jar"  #  (file java che servono per comunicazione tra kafka e spark)
#     )\
#     .config("spark.sql.shuffle.partitions", "1") \
#     .getOrCreate()


# spark.sparkContext.setLogLevel("INFO")


# spark = SparkSession.builder \
#     .appName("CDC_Employee") \
#     .master("local[*]") \
#     .config("spark.jars", "file:///C:/Users/ale/etl_cdc_project/jars/spark-sql-kafka-0-10_2.12-3.4.1.jar,"
#                            "file:///C:/Users/ale/etl_cdc_project/jars/kafka-clients-3.4.1.jar") \
#     .getOrCreate() # C:/Users/ale/etl_cdc_project/jars/spark-sql-kafka-0-10_2.12-3.4.1.jar

# spark.sparkContext.setLogLevel("INFO")

# print("Spark ready! Version:", spark.version)

# Leggere dal topic di kafka. ricorda che i topik sono i messaggi CDC salvati qui in kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "cdc.public.Employee") \
    .option("startingOffsets", "earliest") \
    .load()
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
    .option("checkpointLocation", "C:/tmp_spark/checkpoint") \
    .start()

# Quando usi lo streaming (readStream + writeStream), Spark deve tenere traccia di quali dati ha già letto e processato. Questo serve per due motivi principali:

# Fault tolerance → Se il tuo job Spark si interrompe (crash, spegnimento del PC, ecc.), quando riparte sa da dove riprendere senza rileggere tutto il topic Kafka.

# Exactly-once processing → Garantisce che ogni messaggio venga processato una sola volta, evitando duplicati o perdite di dati.

# Spark salva queste informazioni in una cartella chiamata checkpoint directory
# spark usa winutils.exe per poter scrivere e creare la cartella checkpoint nel nostro os i file, contenente questi importanti dati correlati a CDC/kafka descritti sopra.
# autoriziamo questi permessi di scrittura delle root C:\ mkdir C:\tmp_spark, e poi provando cosi: winutils.exe ls C:\tmp_spark
# se d--------- 1 YOUR_USER YOUR_USER 0 Jan 23 2026 C:\tmp_spark, allora possiamo usare questa cartella senza problemi
# perche in in questa cartella, spark e winutils.exe hanno i diritti di scrivere e creare la tabella checkpoint
query.awaitTermination()
# In questo modo vedi i messaggi CDC in tempo reale, man mano che vengono generati da Debezium