from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, min, max, stddev, to_date, year, month

# 1. Inicializar la sesión de Spark
spark = SparkSession.builder \
    .appName("BatchProcessingTRM") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("\n" + "="*50)
print("=== 1. CARGA DEL CONJUNTO DE DATOS (BATCH) ===")
print("="*50)

# Cargar el archivo CSV
df = spark.read.csv("rows.csv", header=True, inferSchema=True)
df.show(5)

print("\n" + "="*50)
print("=== 2. ESQUEMA ORIGINAL ===")
print("="*50)
df.printSchema()

print("\n" + "="*50)
print("=== 3. LIMPIEZA Y TRANSFORMACIÓN ===")
print("="*50)

# Filtrar registros nulos y convertir tipos de datos
df_clean = df.filter(col("VALOR").isNotNull() & col("VIGENCIADESDE").isNotNull()) \
             .withColumn("VALOR", col("VALOR").cast("double")) \
             .withColumn("FECHA", to_date(col("VIGENCIADESDE"), "dd/MM/yyyy")) \
             .withColumn("ANIO", year(col("FECHA"))) \
             .withColumn("MES", month(col("FECHA")))

print(f"Total registros limpios: {df_clean.count()}")
df_clean.select("FECHA", "VALOR", "ANIO", "MES").show(5)

print("\n" + "="*50)
print("=== 4. ANÁLISIS EXPLORATORIO DE DATOS (EDA) ===")
print("="*50)

# Métricas generales de la variable VALOR
print("--- Estadísticas Generales del VALOR de la TRM ---")
df_clean.select(
    avg("VALOR").alias("Promedio_TRM"),
    min("VALOR").alias("Minimo_TRM"),
    max("VALOR").alias("Maximo_TRM"),
    stddev("VALOR").alias("Desviacion_TRM")
).show()

# Agrupación y agregación por Año
print("--- Promedio y Rangos de TRM agrupados por Año ---")
anual_stats = df_clean.groupBy("ANIO") \
                      .agg(
                          avg("VALOR").alias("Promedio_Anual"),
                          min("VALOR").alias("Minimo_Anual"),
                          max("VALOR").alias("Maximo_Anual")
                      ) \
                      .orderBy("ANIO", ascending=False)

anual_stats.show(10)

spark.stop()
print("=== PROCESAMIENTO EN BATCH FINALIZADO EXITOSAMENTE ===")
