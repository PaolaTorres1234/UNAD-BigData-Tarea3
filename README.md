# UNAD - Big Data (Tarea 3)
## Procesamiento en Batch y Streaming con Apache Spark y Apache Kafka

Este repositorio contiene los desarrollos prácticos correspondientes a la Tarea 3 del curso de Big Data de la UNAD, cubriendo procesamiento por lotes (Batch) y en tiempo real (Streaming).

---

### Contenido del Repositorio

* **`batch_analysis.py`**: Script en PySpark que realiza la ingesta, limpieza, tipificación y análisis exploratorio de datos (EDA) sobre el dataset histórico de la Tasa Representativa del Mercado (TRM). Calcula métricas descriptivas globales y agregaciones temporales por año.
* **`kafka_producer.py`**: Productor en Python que simula la emisión continua de métricas de sensores IoT (temperatura, humedad y timestamp) hacia el topic `sensor_data` de Apache Kafka.
* **`spark_streaming_consumer.py`**: Consumidor estructurado en PySpark (Spark Streaming) conectado a Kafka. Procesa el flujo continuo mediante ventanas de tiempo de 1 minuto, calculando promedios de temperatura y humedad por sensor en tiempo real.

---

### Requisitos del Entorno

* Ubuntu Linux (Máquina Virtual en VirtualBox)
* Apache Hadoop & HDFS
* Apache Spark 3.5.3
* Apache Kafka 3.6.2 & Apache ZooKeeper
* Python 3 con librerías `pyspark` y `kafka-python`

---

### Instrucciones de Ejecución

#### 1. Procesamiento en Batch (PySpark)
Asegurarse de tener el dataset `rows.csv` en la ruta de ejecución y lanzar:
```bash
spark-submit batch_analysis.py
