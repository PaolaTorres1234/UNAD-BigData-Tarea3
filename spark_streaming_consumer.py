from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, to_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, LongType

# 1. Crear sesión de Spark
spark = SparkSession.builder \
    .appName("KafkaSparkStreamingConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Definir el esquema JSON entrante
schema = StructType([
    StructField("sensor_id", IntegerType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("timestamp", LongType(), True)
])

# 3. Leer stream desde el topic de Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Deserializar el payload JSON y formatear timestamp
df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", to_timestamp(col("timestamp")))

# 5. Agrupación por ventana temporal de 1 minuto y cálculo de promedios
df_aggregated = df_parsed \
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("sensor_id")
    ) \
    .agg(
        {"temperature": "avg", "humidity": "avg"}
    )

# 6. Salida continua por consola
query = df_aggregated.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
